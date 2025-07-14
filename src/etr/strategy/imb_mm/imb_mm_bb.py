import time, datetime
import numpy as np
import pandas as pd
import pytz
from copy import deepcopy
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


class ImbMM_BB(StrategyBase):
    """
    - Imbalance > theta => entry
    - Others Vol > theta => 逃げる
    """
    def __init__(
        self,
        sym: str,
        venue: str,
        amount: float,
        client: ExchangeClientBase,
        base_spread: float = 0.0,
        spread_threshold: float = 3,
        exit_offset: float = 0,
        vol_coef: float = 10,
        ret_coef: float = 5,
        sl_level: float = 1,
        tp_level: float = np.nan,
        decimal=0,
        references={
            ("bitbank", "BTCJPY"): {"target_amount": 0.5, "alpha": 0.1, },
            ("bitmex", "XBTUSD"): {"target_amount": 0.5, "alpha": 0.1, },
            ("binance", "BTCUSDT"): {"target_amount": 0.5, "alpha": 0.1, },
        },
        model_id="ImbMM_BB",
        log_file=None,
    ):
        super().__init__(model_id, log_file)
        # parameters
        self.sym = sym
        self.venue = venue
        self.client = client
        self.amount = amount
        self.exit_offset = exit_offset
        self.sl_level = sl_level
        self.tp_level = tp_level
        self.decimal = decimal
        self.references = references
        self.base_spread = base_spread
        self.spread_threshold = spread_threshold
        self.vol_coef = vol_coef
        self.ret_coef = ret_coef

        # cep
        self.ohlc: Dict[Tuple, OHLCV] = {
            (v, s): OHLCV(sym=s, venue=v, log_file=log_file, interval=1, cache_duration=60)
            for (v, s) in references}
        self.ema: Dict[Tuple, EMARealTime] = {
            (v, s): EMARealTime(alpha=references[(v, s)]["alpha"], sym=s, venue=v, log_file=log_file)
            for (v, s) in references}
        self.impact_price: Dict[Tuple, ImpactPrice] = {
            (v, s): ImpactPrice(target_amount=references[(v, s)]["target_amount"], sym=s, venue=v, use_term_amount=False, log_file=log_file)
            for (v, s) in references if v != "binance"}

        # trading attributes
        self.entry_order: Order = Order.null_order()
        self.exit_order: Order = Order.null_order()
        self.latest_rate: Dict[Tuple[str, str], Dict[str, float]] = {}  # (venue, sym) -> {Rate}
        self.warmup_done = False

        # misc
        self._log_timestamp = datetime.datetime.now()

    async def warmup(self, now=datetime.datetime.now(datetime.timezone.utc)):
        tkynow = pd.Timestamp(now).astimezone(pytz.timezone("Asia/Tokyo"))
        if tkynow.hour in [10]:
            raise RuntimeError("Do not turn on strategy during EoD process.")

        self.logger.info("Warming up ImbMM strategy. Loading ratest rates...")
        rates = pd.concat([load_data(date=now.date(), table="Rate", venue=venue, symbol=symbol) for venue, symbol in self.references])
        books = pd.concat([load_data(date=now.date(), table="MarketBook", venue=venue, symbol=symbol) for venue, symbol in self.references])
        rates = rates.loc[(now - datetime.timedelta(minutes=10) < rates.timestamp) & (rates.timestamp <= now)].sort_values("timestamp").assign(_data_type="Rate")
        books = books.loc[(now - datetime.timedelta(minutes=10) < books.timestamp) & (books.timestamp <= now)].sort_values("timestamp").assign(_data_type="MarketBook")
        self.logger.info(f"Loaded rate data {rates.timestamp.min()} - {rates.timestamp.max()}")
        self.logger.info(f"Loaded book data {books.timestamp.min()} - {books.timestamp.max()}")
        rate_msg = list(zip(rates.timestamp, rates.to_dict(orient="records")))
        book_msg = list(zip(books.timestamp, books.to_dict(orient="records")))
        messages = rate_msg + book_msg
        messages = sorted(messages, key=lambda x: x[0])

        for t, msg in messages:
            await self.on_message(msg)

        self.warmup_done = True
        self.logger.info("Warmup done.")

    @property
    def cur_pos(self):
        return self.client.positions.get(self.sym, [0, 0])[1]

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
        mid = self.latest_rate["mid_price"]
        bid = self.latest_rate["best_bid"]
        ask = self.latest_rate["best_ask"]
        spread = self.base_spread
        # spread = spread + np.mean([cep.ema_vol for cep in self.ema.values()]) * self.vol_coef  # volatility
        # spread = spread + mid * np.mean([cep.impact_spread / 2 / 1e4 for cep in self.impact_price.values()])  # impact spread
        price = mid + spread * (-1 * side)
        price = price + np.mean([cep.ema_ret for cep in self.ema.values()]) * self.ret_coef
        if side > 0:
            price = self.my_floor(min(bid, price))
        else:
            price = self.my_ceil(min(ask, price))
        return price

    async def on_message(self, msg: Dict):
        dtype: str = msg.get("_data_type")
        if dtype.startswith("BT_") or dtype == "Heartbeat":
            return

        # update cep
        for cep in self.impact_price.values():
            await cep.on_message(msg)
        for cep in self.ema.values():
            await cep.on_message(msg)

        # update attrs
        if dtype == "Rate" and msg["venue"] == self.venue and msg["sym"] == self.sym:
            self.latest_rate = msg

        # escape
        if not self.warmup_done:
            return

        # heartbeat
        if self._log_timestamp + datetime.timedelta(seconds=60) < datetime.datetime.now():
            self._log_timestamp = datetime.datetime.now()
            self.logger.info("heartbeat")

        # realtime logic
        if dtype == "Order":
            # update order status
            if msg["order_id"] == self.entry_order.order_id:
                msg.pop("_data_type")
                self.entry_order = Order(**msg)
                self.logger.info(f"Updated entry order status || {self.entry_order.to_dict()}")
                if self.entry_order.order_status in [OrderStatus.Filled]:
                    self.logger.info("ENTRY LIMIT ORDER FILLED")
                    self.logger.info("Place exit limit order")
                    await self._place_exit_order(msg, dtype)

            if msg["order_id"] == self.exit_order.order_id:
                msg.pop("_data_type")
                self.exit_order = Order(**msg)
                self.logger.info(f"Updated exit order status || {self.exit_order.to_dict()}")
                if self.exit_order.order_status in [OrderStatus.Filled]:
                    self.logger.info("EXIT LIMIT ORDER FILLED")

        else:
            if self.cur_pos == 0:
                # update bid/ask for entry
                await self._place_entry_order(msg, dtype)
            else:
                # update bid/ask for exit
                await self._place_exit_order(msg, dtype)

    async def _place_entry_order(self, msg: Dict, dtype: str):
        # chack impact spread
        sufficient_imb = abs(self.impact_price[(self.venue, self.sym)].spread_imbalance) > self.spread_threshold
        imbalance_side = np.sign(self.impact_price[(self.venue, self.sym)].spread_imbalance)
        ema_condition = all([imbalance_side * (abs(cep.ema_ret) > 0.05) * np.sign(cep.ema_ret) >= 0 for cep in self.ema.values()])

        if sufficient_imb and ema_condition:
            side = np.sign(self.impact_price[(self.venue, self.sym)].spread_imbalance)
            cur_side = self.entry_order.side * self.entry_order.is_live
            new_price = self.pricing(side)

            if cur_side == 0:
                # no live order
                self.logger.info(f"Place entry limit order @({side}, {new_price})")
                self.entry_order = await self.client.send_order(
                    msg["timestamp"], self.sym, side=side, price=new_price, amount=self.amount, order_type=OrderType.Limit,
                    src_type=dtype, src_timestamp=msg["timestamp"], src_id=msg["universal_id"], misc="entry")
            else:
                place_new = False
                if side * cur_side < 0:
                    # opposite side
                    self.logger.info(f"cancel entry limit order {self.entry_order.order_id}")
                    self.entry_order = await self.client.cancel_order(
                        self.entry_order.order_id,
                        timestamp=msg["timestamp"],
                        src_type=dtype,
                        src_timestamp=msg["timestamp"],
                        src_id=msg["universal_id"],
                    )
                    place_new = True

                elif side == cur_side:
                    # live order in same side
                    is_cur_price_far = abs(new_price / self.entry_order.price - 1) * 1e4 > 0.1
                    is_cur_price_old = (msg["timestamp"] - self.entry_order.timestamp > datetime.timedelta(seconds=1))
                    if is_cur_price_far and is_cur_price_old:
                        self.entry_order = await self.client.cancel_order(
                            self.entry_order.order_id,
                            timestamp=msg["timestamp"],
                            src_type=dtype,
                            src_timestamp=msg["timestamp"],
                            src_id=msg["universal_id"],
                        )
                        place_new = True

                if place_new:
                    self.logger.info(f"Place entry limit order @({side}, {new_price})")
                    self.entry_order = await self.client.send_order(
                        msg["timestamp"], self.sym, side=side, price=new_price, amount=self.amount, order_type=OrderType.Limit,
                        src_type=dtype, src_timestamp=msg["timestamp"], src_id=msg["universal_id"], misc="entry")

        elif self.entry_order.is_live:
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
            if self.latest_rate["best_bid"] < self.latest_rate["best_ask"] - 10 ** self.decimal:
                ask = self.latest_rate["best_ask"] - 10 ** self.decimal
            else:
                ask = self.latest_rate["best_ask"]
            new_ask = self.my_ceil((1 + self.exit_offset / 1e4) * ask)
            use_existing = (
                self.exit_order.is_live
                and self.exit_order.side < 0
                and abs(new_ask / self.exit_order.price - 1) * 1e4 < 0.01
                and (msg["timestamp"] - self.exit_order.timestamp) < datetime.timedelta(seconds=1)
            )
            if not use_existing:
                if self.exit_order.is_live:
                    self.logger.info(f"cancel exit limit order {self.exit_order.order_id}")
                    self.exit_order = await self.client.cancel_order(
                        self.exit_order.order_id, timestamp=msg["timestamp"], src_type=dtype, src_timestamp=msg["timestamp"], src_id=msg["universal_id"])
                if not self.exit_order.is_live:
                    self.logger.info("send exit limit order")
                    self.exit_order = await self.client.send_order(
                        msg["timestamp"], self.sym, side=-1, price=new_ask, amount=self.amount, order_type=OrderType.Limit,
                        src_type=dtype, src_timestamp=msg["timestamp"], src_id=msg["universal_id"], misc="exit")
                else:
                    self.logger.info("exit order might be already canceled or filled, skip sending new exit order.")

        elif self.cur_pos < 0:
            if self.latest_rate["best_ask"] > self.latest_rate["best_bid"] + 10 ** self.decimal:
                bid = self.latest_rate["best_bid"] + 10 ** self.decimal
            else:
                bid = self.latest_rate["best_bid"]
            new_bid = self.my_ceil((1 - self.exit_offset / 1e4) * bid)
            use_existing = (
                self.exit_order.is_live
                and self.exit_order.side > 0
                and abs(new_bid / self.exit_order.price - 1) * 1e4 < 0.01
                and (msg["timestamp"] - self.exit_order.timestamp) < datetime.timedelta(seconds=1)
            )
            if not use_existing:
                if self.exit_order.is_live:
                    self.logger.info(f"cancel exit limit order {self.exit_order.order_id}")
                    self.exit_order = await self.client.cancel_order(
                        self.exit_order.order_id, timestamp=msg["timestamp"], src_type=dtype, src_timestamp=msg["timestamp"], src_id=msg["universal_id"])
                if not self.entry_order.is_live:
                    self.logger.info("send exit limit order")
                    self.exit_order = await self.client.send_order(
                        msg["timestamp"], self.sym, side=+1, price=new_bid, amount=self.amount, order_type=OrderType.Limit,
                        src_type=dtype, src_timestamp=msg["timestamp"], src_id=msg["universal_id"], misc="exit")
                else:
                    self.logger.info("exit order might be already canceled or filled, skip sending new exit order.")
