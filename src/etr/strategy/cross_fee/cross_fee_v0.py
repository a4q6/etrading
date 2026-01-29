import time, datetime
import numpy as np
import pandas as pd
import pytz
from uuid import uuid4
from abc import ABC, abstractmethod
from typing import Union, List, Dict, Callable, Tuple, Optional
from collections import deque
from etr.common.logger import LoggerFactory
from etr.core.api_client.base import ExchangeClientBase
from etr.core.datamodel import Order, OrderStatus, OrderType
from etr.strategy import StrategyBase
from etr.data.data_loader import load_data
from etr.core.cep import OHLCV, ImpactPrice, EMARealTime


class CrossFee(StrategyBase):
    """
    - Partial Fill対応
    """
    def __init__(
        self,
        sym: str,
        venue: str,
        hedge_sym: str,
        hedge_venue: str,
        amount: float,
        client: ExchangeClientBase,
        hedge_client: ExchangeClientBase,
        decimal=0,
        model_id="CrossFee",
        log_file=None,
        dev_sign=+1,
        references={
            ("bitmex", "XBTUSD"): {"alpha": 0.1, },
            ("binance", "BTCUSDT"): {"alpha": 0.1, },
        },
        ema_theta=1,
        legacy=False,
    ):
        super().__init__(model_id, log_file)
        # parameters
        self.sym = sym
        self.venue = venue
        self.client = client
        self.hedge_client = hedge_client
        self.amount = amount
        self.hedge_venue = hedge_venue
        self.hedge_sym = hedge_sym
        self.decimal = decimal
        self.dev_sign = dev_sign
        self.ema_theta = ema_theta
        self.references = references

        # trading attributes
        self.entry_order: Order = Order.null_order()
        self.exit_order: Order = Order.null_order()
        self.hedge_entry: Order = Order.null_order()
        self.hedge_exit: Order = Order.null_order()
        self.latest_rate: Dict[str, float] = {}
        self.hedge_rate: Dict[str, float] = {}
        self.latest_execution = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
        self.warmup_done = False

        # indicators
        self.ema: Dict[Tuple, EMARealTime] = {
            (v, s): EMARealTime(alpha=references[(v, s)]["alpha"], sym=s, venue=v, log_file="cep.log")
            for (v, s) in references}

        # misc
        self._log_timestamp = datetime.datetime.now()

    async def warmup(self, now=datetime.datetime.now(datetime.timezone.utc)):
        # tkynow = pd.Timestamp(now).astimezone(pytz.timezone("Asia/Tokyo"))
        # if tkynow.hour in [10]:
        #     raise RuntimeError("Do not turn on strategy during EoD process.")

        self.logger.info(f"Warming up {self.model_id} strategy. Loading ratest rates...")
        rates = pd.concat([
            load_data(date=now.date(), table="Rate", venue=venue, symbol=symbol)
            for venue, symbol in [(self.venue, self.sym), (self.hedge_venue, self.hedge_sym)] + list(self.references.keys())
        ])
        # books = pd.concat([load_data(date=now.date(), table="MarketBook", venue=venue, symbol=symbol) for venue, symbol in [(self.venue, self.sym)]])
        rates = rates.loc[(now - datetime.timedelta(minutes=10) < rates.timestamp) & (rates.timestamp <= now)].sort_values("timestamp").assign(_data_type="Rate")
        # books = books.loc[(now - datetime.timedelta(minutes=10) < books.timestamp) & (books.timestamp <= now)].sort_values("timestamp").assign(_data_type="MarketBook")
        self.logger.info(f"Loaded rate data {rates.timestamp.min()} - {rates.timestamp.max()}")
        # self.logger.info(f"Loaded book data {books.timestamp.min()} - {books.timestamp.max()}")
        rate_msg = list(zip(rates.timestamp, rates.to_dict(orient="records")))
        # book_msg = list(zip(books.timestamp, books.to_dict(orient="records")))
        messages = rate_msg
        messages = sorted(messages, key=lambda x: x[0])

        for t, msg in messages:
            await self.on_message(msg)

        self.warmup_done = True
        self.logger.info("Warmup done.")

    @property
    def cur_pos(self):
        return self.client.positions.get(self.sym, [0, 0])[1]

    @property
    def hedge_pos(self):
        return self.hedge_client.positions.get(self.hedge_sym, [0, 0])[1]

    def my_ceil(self, num: float) -> float:
        factor = 10 ** self.decimal
        return np.ceil(num * factor) / factor

    def my_floor(self, num: float) -> float:
        factor = 10 ** self.decimal
        return np.floor(num * factor) / factor

    def pricing(self, side: int) -> float:
        """
            BID if imbalance > 0
        """
        # mid = self.latest_rate["mid_price"]
        bid = self.latest_rate["best_bid"]
        ask = self.latest_rate["best_ask"]
        # spread = spread + np.mean([cep.ema_vol for cep in self.ema.values()]) * self.vol_coef  # volatility
        # spread = spread + mid * np.mean([cep.impact_spread / 2 / 1e4 for cep in self.impact_price.values()])  # impact spread
        if side > 0:
            price = bid
        else:
            price = ask
        return price

    async def on_message(self, msg: Dict):
        dtype: str | None = msg.get("_data_type")
        if dtype is None or dtype.startswith("BT_") or dtype == "Heartbeat":
            return

        # update attrs
        if dtype == "Rate" and msg["venue"] == self.venue and msg["sym"] == self.sym:
            self.latest_rate = msg
        if dtype == "Rate" and msg["venue"] == self.hedge_venue and msg["sym"] == self.hedge_sym:
            self.hedge_rate = msg

        for cep in self.ema.values():
            await cep.on_message(msg)

        # escape
        if not self.warmup_done:
            return

        # realtime logic
        if dtype == "Order":
            # update order status
            if msg["order_id"] == self.entry_order.order_id:
                msg.pop("_data_type")
                self.entry_order = Order(**msg)
                self.logger.info(f"Updated entry order status (order_id = {self.entry_order.order_id}, {self.entry_order.order_status})")
                if self.entry_order.order_status in [OrderStatus.Filled, OrderStatus.Partial]:
                    self.logger.info("ENTRY LIMIT ORDER FILLED")
                    self.latest_execution = msg["timestamp"]
                    async with self.entry_order.lock:
                        async with self.exit_order.lock:
                            await self.client.cancel_all_orders(timestamp=msg["timestamp"], sym=self.sym)

            if msg["order_id"] == self.exit_order.order_id:
                msg.pop("_data_type")
                self.exit_order = Order(**msg)
                self.logger.info(f"Updated exit order status || {self.exit_order.to_dict()}")
                if self.exit_order.order_status in [OrderStatus.Filled, OrderStatus.Partial]:
                    self.logger.info("EXIT LIMIT ORDER FILLED")
                    self.latest_execution = msg["timestamp"]
                    async with self.entry_order.lock:
                        async with self.exit_order.lock:
                            await self.client.cancel_all_orders(timestamp=msg["timestamp"], sym=self.sym)

            if msg["order_id"] == self.hedge_entry.order_id:
                msg.pop("_data_type")
                self.hedge_entry = Order(**msg)
                self.logger.info(f"Updated entry order status (order_id = {self.hedge_entry.order_id}, {self.hedge_entry.order_status})")
                if self.hedge_entry.order_status in [OrderStatus.Filled, OrderStatus.Partial]:
                    self.logger.info("ENTRY HEDGE ORDER FILLED")
                    self.latest_execution = msg["timestamp"]

            if msg["order_id"] == self.hedge_exit.order_id:
                msg.pop("_data_type")
                self.hedge_exit = Order(**msg)
                self.logger.info(f"Updated exit order status || {self.hedge_exit.to_dict()}")
                if self.hedge_exit.order_status in [OrderStatus.Filled, OrderStatus.Partial]:
                    self.logger.info("EXIT HEDGE ORDER FILLED")
                    self.latest_execution = msg["timestamp"]

        elif dtype == "Trade":
            if msg["order_id"] == self.entry_order.order_id:
                self.logger.info("Entry order trade message arrived")
                self.latest_execution = msg["timestamp"]
                # add hedge position
                async with self.hedge_entry.lock:
                    self.hedge_entry = await self.hedge_client.send_order(
                        msg["timestamp"], sym=self.hedge_sym, side=msg["side"] * -1, price=None, amount=msg["amount"],
                        order_type=OrderType.Market, src_type=msg["_data_type"], src_timestamp=msg["timestamp"], src_id=msg["universal_id"])
            if msg["order_id"] == self.exit_order.order_id:
                self.logger.info("Exit order trade message arrived")
                self.latest_execution = msg["timestamp"]
                # decrease hedge position
                async with self.hedge_exit.lock:
                    self.hedge_exit = await self.hedge_client.send_order(
                        msg["timestamp"], sym=self.hedge_sym, side=msg["side"] * -1, price=None, amount=msg["amount"],
                        order_type=OrderType.Market, src_type=msg["_data_type"], src_timestamp=msg["timestamp"], src_id=msg["universal_id"])
            if msg["order_id"] == self.hedge_entry.order_id:
                self.logger.info("Entry hedge trade message arrived")
                self.latest_execution = msg["timestamp"]
            if msg["order_id"] == self.hedge_exit.order_id:
                self.logger.info("Exit hedge trade message arrived")
                self.latest_execution = msg["timestamp"]

        elif msg["timestamp"] > self.latest_execution + datetime.timedelta(seconds=0.5):
            if self.cur_pos == 0:
                # cancel exit limit
                if self.exit_order.is_live and not self.exit_order.is_locked:
                    async with self.exit_order.lock:
                        self.logger.info(f"cancel unnecessary exit limit order {self.exit_order.order_id}")
                        self.exit_order = await self.client.cancel_order(
                            self.exit_order.order_id, timestamp=msg["timestamp"], src_type=dtype, src_timestamp=msg["timestamp"], src_id=msg["universal_id"])

                # update bid/ask for entry
                await self._place_entry_order(msg, dtype)
            else:
                # update bid/ask for exit
                await self._place_exit_order(msg, dtype)

        # heartbeat
        if self._log_timestamp + datetime.timedelta(seconds=60) < datetime.datetime.now():
            self._log_timestamp = datetime.datetime.now()
            self.logger.info("heartbeat")

    async def _place_entry_order(self, msg: Dict, dtype: str):
        # check current price deviation
        dev = (self.latest_rate["mid_price"] / self.hedge_rate["mid_price"] - 1) * 1e4

        if self.ema["binance", "BTCUSDT"].ema_vol < self.ema_theta:
            side = np.sign(dev) * (self.dev_sign)
            cur_side = self.entry_order.side * self.entry_order.is_live
            new_price = self.pricing(side)

            if cur_side == 0 and msg["timestamp"] > self.entry_order.timestamp + datetime.timedelta(seconds=0.3):
                # no live order
                if not self.entry_order.is_locked:
                    async with self.entry_order.lock:
                        self.logger.info(f"Place entry limit order @({side}, {new_price}, {self.amount})")
                        self.entry_order = await self.client.send_order(
                            msg["timestamp"], self.sym, side=side, price=new_price, amount=self.amount, order_type=OrderType.Limit,
                            src_type=dtype, src_timestamp=msg["timestamp"], src_id=msg["universal_id"], misc="entry")
            else:
                # replace or keep
                place_new = False
                if side * cur_side < 0:
                    # opposite side
                    # cancel existing
                    if not self.entry_order.is_locked:
                        async with self.entry_order.lock:
                            self.logger.info(f"cancel entry limit order {self.entry_order.order_id}")
                            self.entry_order = await self.client.cancel_order(
                                self.entry_order.order_id,
                                timestamp=msg["timestamp"],
                                src_type=dtype,
                                src_timestamp=msg["timestamp"],
                                src_id=msg["universal_id"],
                            )
                            place_new = self.entry_order.order_status not in (OrderStatus.Filled, OrderStatus.Partial)

                elif side == cur_side:
                    # live order in same side
                    is_cur_price_far = abs(new_price / self.entry_order.price - 1) * 1e4 > 0.1
                    is_cur_price_old = (msg["timestamp"] - self.entry_order.timestamp > datetime.timedelta(seconds=0.3))
                    # cancel existing
                    if is_cur_price_far and is_cur_price_old:
                        if not self.entry_order.is_locked:
                            async with self.entry_order.lock:
                                self.entry_order = await self.client.cancel_order(
                                    self.entry_order.order_id,
                                    timestamp=msg["timestamp"],
                                    src_type=dtype,
                                    src_timestamp=msg["timestamp"],
                                    src_id=msg["universal_id"],
                                )
                                place_new = self.entry_order.order_status not in (OrderStatus.Filled, OrderStatus.Partial)

                if place_new:
                    if not self.entry_order.is_locked:
                        async with self.entry_order.lock:
                            self.logger.info(f"Place entry limit order @({side}, {new_price}, {self.amount})")
                            self.entry_order = await self.client.send_order(
                                msg["timestamp"], self.sym, side=side, price=new_price, amount=self.amount, order_type=OrderType.Limit,
                                src_type=dtype, src_timestamp=msg["timestamp"], src_id=msg["universal_id"], misc="entry")

        elif self.entry_order.is_live and msg["timestamp"] > self.entry_order.timestamp + datetime.timedelta(seconds=0.3):
            if not self.entry_order.is_locked:
                async with self.entry_order.lock:
                    self.logger.info(f"cancel entry limit order {self.entry_order.order_id}")
                    self.entry_order = await self.client.cancel_order(
                        self.entry_order.order_id,
                        timestamp=msg["timestamp"],
                        src_type=dtype,
                        src_timestamp=msg["timestamp"],
                        src_id=msg["universal_id"],
                    )

    async def _place_exit_order(self, msg: Dict, dtype: str):
        if self.cur_pos > 0:
            if self.latest_rate["best_bid"] < self.latest_rate["best_ask"] - 10 ** (-self.decimal):
                ask = self.latest_rate["best_ask"] - 10 ** (-self.decimal)
            else:
                ask = self.latest_rate["best_ask"]
            new_ask = ask
            if not self.exit_order.is_locked:
                async with self.exit_order.lock:
                    use_existing = (
                        self.exit_order.is_live
                        and self.exit_order.side < 0
                        and abs(new_ask / self.exit_order.price - 1) * 1e4 < 0.01
                        # and (msg["timestamp"] - self.exit_order.timestamp) < datetime.timedelta(seconds=1)
                    )
                    if not use_existing:
                        if self.exit_order.is_live:
                            self.logger.info(f"cancel exit limit order {self.exit_order.order_id}")
                            self.exit_order = await self.client.cancel_order(
                                self.exit_order.order_id, timestamp=msg["timestamp"], src_type=dtype, src_timestamp=msg["timestamp"], src_id=msg["universal_id"])
                        if not self.exit_order.is_live:
                            self.logger.info(f"send exit limit order @(-1, {new_ask}, {abs(self.cur_pos)})")
                            self.exit_order = await self.client.send_order(
                                msg["timestamp"], self.sym, side=-1, price=new_ask, amount=abs(self.cur_pos), order_type=OrderType.Limit,
                                src_type=dtype, src_timestamp=msg["timestamp"], src_id=msg["universal_id"], misc="exit")

        elif self.cur_pos < 0:
            if self.latest_rate["best_ask"] > self.latest_rate["best_bid"] + 10 ** (-self.decimal):
                bid = self.latest_rate["best_bid"] + 10 ** (-self.decimal)
            else:
                bid = self.latest_rate["best_bid"]
            new_bid = bid
            if not self.exit_order.is_locked:
                async with self.exit_order.lock:
                    use_existing = (
                        self.exit_order.is_live
                        and self.exit_order.side > 0
                        and abs(new_bid / self.exit_order.price - 1) * 1e4 < 0.01
                        # and (msg["timestamp"] - self.exit_order.timestamp) < datetime.timedelta(seconds=1)
                    )
                    if not use_existing:
                        if self.exit_order.is_live:
                            self.logger.info(f"cancel exit limit order {self.exit_order.order_id}")
                            self.exit_order = await self.client.cancel_order(
                                self.exit_order.order_id, timestamp=msg["timestamp"], src_type=dtype, src_timestamp=msg["timestamp"], src_id=msg["universal_id"])
                        if not self.exit_order.is_live:
                            self.logger.info(f"send exit limit order @(+1, {new_bid}, {abs(self.cur_pos)})")
                            self.exit_order = await self.client.send_order(
                                msg["timestamp"], self.sym, side=+1, price=new_bid, amount=abs(self.cur_pos), order_type=OrderType.Limit,
                                src_type=dtype, src_timestamp=msg["timestamp"], src_id=msg["universal_id"], misc="exit")
        else:
            if msg["timestamp"].second == 0:
                self.logger.info("No position, skip exit order")
