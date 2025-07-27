import datetime
import numpy as np
import pandas as pd
from dataclasses import dataclass
from uuid import uuid4
from typing import Union, List, Dict, Callable, Set, Optional, Any
from copy import copy

from etr.core.datamodel import OrderType, OrderStatus, Order, BacktestOrder, Trade, Rate
from etr.core.api_client.base import ExchangeClientBase
from etr.strategy.base_strategy import StrategyBase


class BacktestClient(ExchangeClientBase):

    def __init__(
        self,
        venue: str,
        min_amount_decimal=4,
    ):
        self.venue = venue
        self.min_amount_decimal = min_amount_decimal
        self.strategy: StrategyBase = None

        self.new_order_message: Dict[str, Order] = {}
        self.amend_order_message: Dict[str, Order] = {}
        self.cancel_order_message: Dict[str, Order] = {}
        self.filled_orders_message: Dict[str, Order] = {}
        self.open_limit_orders: Dict[str, Order] = {}
        self.pending_limit_orders: Dict[str, Order] = {}
        self.open_stop_orders: Dict[str, Order] = {}
        self.open_market_orders: Dict[str, Order] = {}
        self.closed_pnl = 0
        self.open_pnl = {}
        self.positions = {}  # {sym: (vwap, amount)}
        self.transactions: List[Trade] = []
        self._latest_book: Dict[str, Any] = {}

    def register_strategy(self, strategy: StrategyBase):
        self.strategy = strategy

    async def send_order(
        self,
        timestamp: datetime.datetime,
        sym: str,
        side: int,
        price: Optional[float],
        amount: float,
        order_type: OrderType,
        src_type: str,
        src_timestamp: datetime.datetime,
        src_id=None,
        misc=None,
        **kwargs
    ) -> BacktestOrder:
        order_info = BacktestOrder(
            timestamp,
            market_created_timestamp=pd.NaT,
            sym=sym,
            side=side,
            price=price,
            amount=amount,
            executed_amount=0,
            order_type=order_type,
            order_status=OrderStatus.New,
            venue=self.venue,
            model_id=self.strategy.model_id,
            process_id=self.strategy.process_id,
            src_type=src_type,
            src_id=src_id,
            src_timestamp=src_timestamp,
            misc=misc,
        )
        order_info.order_id = order_info.universal_id
        if order_type == OrderType.Market:
            self.open_market_orders[order_info.order_id] = order_info
        elif order_type == OrderType.Limit:
            self.pending_limit_orders[order_info.order_id] = order_info
        self.new_order_message[order_info.order_id] = copy(order_info)
        return order_info

    async def cancel_order(
        self,
        order_id,
        timestamp,
        src_type,
        src_timestamp,
        src_id=None,
        misc=None,
        **kwargs
    ) -> BacktestOrder:
        oinfo = self.open_limit_orders[order_id]
        oinfo.timestamp = timestamp
        oinfo.src_type = src_type
        oinfo.src_timestamp = src_timestamp
        oinfo.src_id = src_id
        oinfo.order_status = OrderStatus.Canceled
        oinfo.misc = misc
        self.cancel_order_message[oinfo.order_id] = copy(oinfo)
        return copy(oinfo)

    async def cancel_all_orders(self, timestamp: datetime.datetime, sym: str, side: int = None):
        for oid, o in self.open_limit_orders.items():
            if side is None or side == o.side:
                await self.cancel_order(order_id=oid, timestamp=timestamp, src_type=None, src_timestamp=timestamp)

    async def amend_order(
        self,
        timestamp,
        order_id,
        price,
        amount,
        src_type,
        src_id=None,
        misc=None,
        **kwargs
    ) -> BacktestOrder:
        oinfo = self.open_limit_orders[order_id]
        oinfo.timestamp = timestamp
        oinfo.price = price
        oinfo.amount = amount
        oinfo.src_type = src_type
        oinfo.src_timestamp = timestamp
        oinfo.src_id = src_id
        oinfo.misc = misc
        self.amend_order_message[oinfo.order_id] = copy(oinfo)
        return oinfo

    async def check_order(self, order_id: str) -> BacktestOrder:
        for orders in [self.open_limit_orders, self.open_market_orders]:
            oinfo = orders.get(order_id)
            if oinfo is not None:
                return oinfo
        return Order.null_order()

    async def on_message(self, msg: Dict) -> None:
        dtype: str = msg.get("_data_type")

        self.open_limit_orders.update(self.pending_limit_orders)
        self.pending_limit_orders = {}
        self.open_limit_orders = {oid: o for oid, o in self.open_limit_orders.items() if o.order_status not in (OrderStatus.Canceled, OrderStatus.Filled)}

        if dtype.startswith("BT_"):
            # Check Market Orders
            if dtype == "BT_Rate" and msg.get("venue") == self.venue:
                sym = msg.get("sym")
                filled_ids = []
                for oid, o in self.open_market_orders.items():
                    if o.sym == sym:
                        o.order_status = OrderStatus.Filled
                        o.price = msg["best_bid"] if o.side < 0 else msg["best_ask"]
                        o.executed_amount = o.amount
                        o.timestamp = o.market_created_timestamp = msg["market_created_timestamp"]
                        self.filled_orders_message[oid] = copy(o)
                        t = Trade.from_order(msg["market_created_timestamp"], msg["market_created_timestamp"], price=o.price, trade_id=uuid4().hex, exec_amount=o.executed_amount, order=o)
                        self.update_position(t)
                        self.transactions.append(t)
                        await self.strategy.on_message(t.to_dict(to_string_timestamp=False))
                        await self.strategy.on_message(o.to_dict(to_string_timestamp=False))
                        filled_ids.append(oid)
                self.open_market_orders = {oid: o for oid, o in self.open_market_orders.items() if oid not in filled_ids}

            elif dtype == "BT_MarketTrade" and msg.get("venue") == self.venue:
                # Check Limit Orders
                sym = msg.get("sym")
                for oid, o in self.open_limit_orders.items():
                    if o.sym == sym:
                        if (o.side * msg["side"] < 0) and ((msg["price"] - o.price) * o.side <= 0):
                            filled_amount = 0
                            if msg["price"] == o.price:
                                # potentially filled
                                book_side = "bids" if o.side > 0 else "asks"
                                b = np.array(self._latest_book[book_side].tolist())
                                book_amount = b[(b[:, 0] - msg["price"]) * o.side >= 0, 1].sum()
                                mo_amount = max(msg["amount"] - book_amount, 0)
                                remain_amount = o.amount - o.executed_amount
                                filled_amount = min(remain_amount * (mo_amount / (book_amount + 1e-6)), remain_amount)
                                filled_amount = round(filled_amount, self.min_amount_decimal)
                            else:
                                remain_amount = o.amount - o.executed_amount
                                filled_amount = round(min(remain_amount, msg["amount"]), self.min_amount_decimal)
                            if filled_amount > 0:
                                o.executed_amount += filled_amount
                                o.order_status = OrderStatus.Filled if abs(o.amount - o.executed_amount) < 1e-6 else OrderStatus.Partial
                                o.timestamp = o.market_created_timestamp = msg["market_created_timestamp"]
                                self.filled_orders_message[oid] = copy(o)
                                t: Trade = Trade.from_order(msg["market_created_timestamp"], msg["market_created_timestamp"],
                                                            price=o.price, trade_id=uuid4().hex, exec_amount=filled_amount, order=o)
                                self.update_position(t)
                                self.transactions.append(t)
                                await self.strategy.on_message(o.to_dict(to_string_timestamp=False))
                                await self.strategy.on_message(t.to_dict(to_string_timestamp=False))

                self.open_limit_orders = {oid: o for oid, o in self.open_limit_orders.items() if o.order_status not in (OrderStatus.Canceled, OrderStatus.Filled)}

            elif dtype == "BT_MarketBook":
                self._latest_book = msg

    def update_position(self, trade: Trade) -> None:
        sym = trade.sym
        if sym not in self.positions or self.positions[sym][1] == 0:
            # New
            self.positions[sym] = [trade.price, trade.amount * trade.side]
        else:
            exec_amount = (trade.amount * trade.side)
            cur_size = self.positions[sym][1]
            new_size = cur_size + exec_amount
            if round(new_size, 7) == 0:
                # close
                self.closed_pnl += trade.side * (trade.price - self.positions[sym][0]) * trade.amount
                self.positions[sym] = [np.nan, 0]
            elif exec_amount * cur_size > 0:
                # same side
                self.positions[sym] = [(trade.price * exec_amount) + (self.positions[sym][0] * cur_size) / new_size, new_size]
            elif new_size * cur_size < 0:
                # flip
                self.closed_pnl += cur_size * (trade.price - self.positions[sym][0])
                self.positions[sym] = [trade.price, new_size]
            elif cur_size * exec_amount < 0 and new_size * cur_size > 0:
                # partial close
                self.closed_pnl += trade.side * (trade.price - self.positions[sym][0]) * trade.amount
                self.positions[sym][1] = new_size
            else:
                raise ValueError()

    def update_open_pnl(self, rate: Rate) -> None:
        if rate.sym in self.positions:
            ref_price = rate.mid_price
            self.open_pnl[rate.sym] = (ref_price - self.positions[rate.sym][0]) * self.positions[rate.sym][1]

    @property
    def orders_frame(self) -> pd.DataFrame:
        o = pd.concat([
            pd.DataFrame(o.values())
            for o in [self.new_order_message, self.amend_order_message, self.filled_orders_message, self.cancel_order_message]
        ])
        if o.shape[0] > 0:
            return o.sort_values(["timestamp", "order_id"]).reset_index(drop=True)
        return o

    @property
    def transactions_frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.transactions)
