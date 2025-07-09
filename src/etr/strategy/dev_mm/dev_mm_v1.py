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

class DevMMv1(StrategyBase):
    """
        GlobalMid(t) := Mean(MidPrices(t))
        Dev(t) = Mid(t) / GlobalMid(t)
        MA[Dev(t)] = Avg(Dev | t-window : t)
        Ask(t) = GlobalMid(t) * (MA[Dev(t)] + offset)
        Bid(t) = GlobalMid(t) * (MA[Dev(t)] - offset)

        Vol(t) = Std(delta%(GlobalMid))
    """
    def __init__(
        self,
        sym: str,
        venue: str,
        amount: float,
        client: ExchangeClientBase,
        position_horizon: float,
        sl_level: float,
        tp_level: float,
        entry_offset: float,
        exit_offset: float,
        dev_window=datetime.timedelta(minutes=10),
        reference=[
            ("binance", "BTCUSDT"),
            ("bitflyer", "BTCJPY"),
            ("bitmex", "XBTUSD"),
        ],
        vol_threshold: float = 50,  # [%/Y]
        decimal=0,
        model_id="DevMM_v1",
        log_file=None,
    ):
        super().__init__(model_id, log_file)
        self.sym = sym
        self.venue = venue
        self.client = client
        self.amount = amount
        self.horizon = position_horizon
        self.sl_level = sl_level
        self.tp_level = tp_level
        self.entry_offset = entry_offset
        self.exit_offset = exit_offset
        self.dev_window = dev_window
        self.vol_threshold = vol_threshold
        self.decimal = decimal
        self.reference = reference

        self.entry_order: Order = Order.null_order()
        self.exit_order: Order = Order.null_order()

        self.usdjpy: float = np.nan
        self.mid: float = np.nan  # target venue's mid
        self.latest_rate: Dict[str, float] = {}
        self.latest_mid: Dict[Tuple[str, str], float] = {k: np.nan for k in reference}
        self.latest_gmid: float = np.nan
        self.gmid_hist: deque = deque()
        self.latest_dev: np.float = np.nan
        self.dev_hist: deque = deque()
        self.dev_ma = np.nan
        self._total = 0.0  # for MA(dev) calculation
        self._count = 0  # for MA(dev) calculation
        self.last_update_gmid_hist = datetime.datetime(2000, 1, 1, tzinfo=pytz.timezone("UTC"))
        self.gmid_std = np.nan
        self._vol_cache = []
        self._log_timestamp = datetime.datetime.now()
        self._last_executed_at = pd.Timestamp("2000-01-01", tz="UTC")

    def warmup(self, now=datetime.datetime.now(datetime.timezone.utc)):
        tkynow = pd.Timestamp(now).astimezone(pytz.timezone("Asia/Tokyo"))
        if tkynow.hour in [10]:
            raise RuntimeError("Do not turn on strategy during EoD process.")

        self.logger.info("Warming up DevMM strategy. Loading ratest rates...")
        rates = pd.concat(
            [load_data(date=now.date(), table="Rate", venue=venue, symbol=symbol) for venue, symbol in self.reference
            ] + [load_data(date=now.date(), table="Rate", venue="gmo", symbol="USDJPY")
            ] + [load_data(date=now.date(), table="Rate", venue=self.venue, symbol=self.sym)]
        )
        rates = rates.loc[(now - self.dev_window * 2 < rates.timestamp) & (rates.timestamp <= now)].sort_values("timestamp").assign(_data_type="Rate")
        self.logger.info(f"Loaded rate data {rates.timestamp.min()} - {rates.timestamp.max()}")
        rates_msg = rates.to_dict(orient="records")
        for msg in rates_msg:
            self._update_deviation(msg)
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

    async def on_message(self, msg: Dict):
        dtype = msg.get("_data_type")

        if self._log_timestamp + datetime.timedelta(seconds=60) < datetime.datetime.now():
            self._log_timestamp = datetime.datetime.now()
            self.logger.info("heartbeat")

        if dtype == "Order":
            # update order status
            if msg["order_id"] == self.entry_order.order_id:
                msg.pop("_data_type")
                self.entry_order = Order(**msg)
                self.logger.info(f"Updated entry order status || {self.entry_order.to_dict()}")
                if self.entry_order.order_status in [OrderStatus.Filled]:
                    self.logger.info("ENTRY LIMIT ORDER FILLED")
                    self.logger.info(f"Place exit limit order")
                    await self._place_exit_order(msg, dtype)
                
            if msg["order_id"] == self.exit_order.order_id:
                msg.pop("_data_type")
                self.exit_order = Order(**msg)
                self.logger.info(f"Updated exit order status || {self.exit_order.to_dict()}")
                if self.exit_order.order_status in [OrderStatus.Filled]:
                    self.logger.info("EXIT LIMIT ORDER FILLED")

        if dtype == "Rate":
            # update deviation
            self._update_deviation(msg)

            # update bid/ask for entry
            if self.cur_pos == 0:
                if len(self.gmid_hist) > 10:
                    gmid = np.array(self.gmid_hist)[:, 1]
                    gmid_ma = (gmid[-1] - gmid.mean())
                    gmid_sign = np.sign(gmid_ma)
                    gmid_std = np.std((np.diff(gmid) / gmid[:-1] - 1)) * 100 * np.sqrt(6 * 1440 * 365)
                    is_live_order = self.entry_order.is_live
                    if gmid_std < self.vol_threshold:
                        if gmid_sign > 0 and self.dev_ma < 0:
                            # cur=bid & cur_bidがnot live -> 新規
                            # cur=bid & new_bidとcur_bidの乖離が大きい and live -> キャンセル&更新
                            # cur=bid & new_bidとcur_bidの乖離が小さい and live -> 維持
                            # cur=ask -> キャンセル&更新
                            new_bid = self.my_floor(self.latest_gmid * (1 + (self.dev_ma - self.entry_offset) / 1e4))
                            new_bid = np.minimum(new_bid, self.latest_rate["best_bid"])
                            bidnow = self.entry_order.side > 0
                            is_cur_bid_far = bidnow and abs(new_bid / self.entry_order.price - 1) * 1e4 > 0.5
                            is_cur_bid_old = bidnow and (msg["timestamp"] - self.entry_order.timestamp > datetime.timedelta(seconds=1))
                            if bidnow and ((not is_cur_bid_far) or (not is_cur_bid_old)):
                                pass  # keep existing order
                            else:
                                # cancel
                                if is_live_order:
                                    self.logger.info(f"cancel entry limit order {self.entry_order.order_id}")
                                    self.entry_order = await self.client.cancel_order(
                                        self.entry_order.order_id,
                                        timestamp=msg["timestamp"],
                                        src_type=dtype,
                                        src_timestamp=msg["timestamp"],
                                        src_id=msg["universal_id"],
                                    )
                                # new order
                                self.logger.info(f"Place entry limit order @(+1, {new_bid})")
                                self.entry_order = await self.client.send_order(
                                    msg["timestamp"], self.sym, side=1, price=new_bid, amount=self.amount, order_type=OrderType.Limit,
                                    src_type=dtype, src_timestamp=msg["timestamp"], src_id=msg["universal_id"], misc="entry")

                        elif gmid_sign < 0 and self.dev_ma > 0:
                            new_ask = self.my_ceil(self.latest_gmid * (1 + (self.dev_ma + self.entry_offset) / 1e4))
                            new_ask = np.maximum(new_ask, self.latest_rate["best_ask"])
                            asknow = self.entry_order.side < 0
                            is_cur_ask_far = asknow and abs(new_ask / self.entry_order.price - 1) * 1e4 > 0.5
                            is_cur_ask_old = asknow and (msg["timestamp"] - self.entry_order.timestamp > datetime.timedelta(seconds=1))
                            is_live_order = self.entry_order.is_live
                            if asknow and ((not is_cur_ask_far) or (not is_cur_ask_old)):
                                pass  # keep existing order
                            else:
                                # cancel
                                if is_live_order:
                                    self.logger.info(f"cancel entry limit order {self.entry_order.order_id}")
                                    self.entry_order = await self.client.cancel_order(
                                        self.entry_order.order_id,
                                        timestamp=msg["timestamp"],
                                        src_type=dtype,
                                        src_timestamp=msg["timestamp"],
                                        src_id=msg["universal_id"],
                                    )
                                # new order
                                self.logger.info(f"Place entry limit order @(-1, {new_ask})")
                                self.entry_order = await self.client.send_order(
                                    msg["timestamp"], self.sym, side=-1, price=new_ask, amount=self.amount, order_type=OrderType.Limit,
                                    src_type=dtype, src_timestamp=msg["timestamp"], src_id=msg["universal_id"], misc="entry")
                        elif is_live_order:
                            self.logger.info(f"cancel entry limit order {self.entry_order.order_id}")
                            self.entry_order = await self.client.cancel_order(
                                self.entry_order.order_id,
                                timestamp=msg["timestamp"],
                                src_type=dtype,
                                src_timestamp=msg["timestamp"],
                                src_id=msg["universal_id"],
                            )
                    elif is_live_order:
                        self.logger.info(f"cancel entry limit order {self.entry_order.order_id}")
                        self.entry_order = await self.client.cancel_order(
                            self.entry_order.order_id,
                            timestamp=msg["timestamp"],
                            src_type=dtype,
                            src_timestamp=msg["timestamp"],
                            src_id=msg["universal_id"],
                        )

            else:
                await self._place_exit_order(msg, dtype)

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
                if not self.exit_order.is_live:
                    self.logger.info("send exit limit order")
                    self.exit_order = await self.client.send_order(
                        msg["timestamp"], self.sym, side=+1, price=new_bid, amount=self.amount, order_type=OrderType.Limit,
                        src_type=dtype, src_timestamp=msg["timestamp"], src_id=msg["universal_id"], misc="exit")
                else:
                    self.logger.info("exit order might be already canceled or filled, skip sending new exit order.")

    def _update_deviation(self, msg):
        if msg["sym"] == "USDJPY":
            # USDJPY exchange rate
            self.usdjpy = msg["mid_price"]
        elif msg["venue"] == self.venue and msg["sym"] == self.sym:
            # target venue
            self.mid = msg["mid_price"]
            self.latest_rate = msg
        else:
            # update reference mid
            if "USD" in msg["sym"] and not np.isnan(self.usdjpy):
                self.latest_mid[(msg["venue"], msg["sym"])] = msg["mid_price"] * self.usdjpy
            else:
                self.latest_mid[(msg["venue"], msg["sym"])] = msg["mid_price"]

            if not np.isnan(self.mid) and all(not np.isnan(v) for v in self.latest_mid.values()):
                gmid = np.mean(list(self.latest_mid.values()))
                self.latest_gmid = gmid
                self.latest_dev = (self.mid / gmid - 1) * 1e4
                self.dev_hist.append([msg["timestamp"], self.latest_dev])
                self._total += self.latest_dev
                self._count += 1
                self._trim(self.dev_hist, now=msg["timestamp"], update_cache=True)
                self.dev_ma = self._total / self._count

                if self.last_update_gmid_hist + datetime.timedelta(seconds=10) < msg["timestamp"]:
                    self.last_update_gmid_hist = msg["timestamp"]
                    self.gmid_hist.append([msg["timestamp"], gmid])
                    self._trim(self.gmid_hist, now=msg["timestamp"], update_cache=False)

    def _trim(self, queue: deque, now: datetime.datetime, update_cache=False):
        while queue and (now - queue[0][0]) > self.dev_window:
            old_time, old_value = queue.popleft()
            if update_cache:
                self._total -= old_value
                self._count -= 1
