import datetime
import numpy as np
import pandas as pd
from dataclasses import dataclass
from uuid import uuid4
from typing import Union, List, Dict, Callable, Set, Optional, Any, Tuple
from copy import copy

from etr.core.datamodel import OrderType, OrderStatus, Order, BacktestOrder, Trade, Rate
from etr.core.api_client.base import ExchangeClientBase
from etr.strategy.base_strategy import StrategyBase


class BacktestClient(ExchangeClientBase):

    def __init__(
        self,
        venue: str,
        min_amount_decimal=4,
        order_latency: float = 0.100,   # seconds: New -> Sent
        cancel_latency: float = 0.100,  # seconds: PendingCancel -> Canceled
        amend_latency: float = 0.100,   # seconds: PendingUpdate -> Updated
    ):
        self.venue = venue
        self.min_amount_decimal = min_amount_decimal
        self.order_latency = order_latency
        self.cancel_latency = cancel_latency
        self.amend_latency = amend_latency
        self.strategy: StrategyBase = None

        # order state dicts
        self.pending_new_orders: Dict[str, BacktestOrder] = {}          # New: waiting for order_latency
        self.open_limit_orders: Dict[str, BacktestOrder] = {}           # Sent/Partial/Updated on book
        self.open_market_orders: Dict[str, BacktestOrder] = {}          # Sent market orders
        self.open_stop_orders: Dict[str, BacktestOrder] = {}            # Sent stop orders
        # pending_cancel: {order_id: (order, deadline)}
        self.pending_cancel_orders: Dict[str, Tuple[BacktestOrder, Any]] = {}
        # pending_amend: {order_id: (order, deadline, new_price, new_amount)}
        self.pending_amend_orders: Dict[str, Tuple[BacktestOrder, Any, float, float]] = {}

        # message log dicts (for orders_frame)
        self.new_order_message: Dict[str, BacktestOrder] = {}
        self.sent_order_message: Dict[str, BacktestOrder] = {}
        self.amend_order_message: Dict[str, BacktestOrder] = {}
        self.cancel_order_message: Dict[str, BacktestOrder] = {}
        self.filled_orders_message: Dict[str, BacktestOrder] = {}
        # full event log: every status transition in chronological order
        self.order_events: List[BacktestOrder] = []

        self.pending_positions = False  # always False in backtest
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
        # All order types wait for order_latency before hitting the book
        self.pending_new_orders[order_info.order_id] = order_info
        _snapshot = copy(order_info)
        self.new_order_message[order_info.order_id] = _snapshot
        self.order_events.append(_snapshot)
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
        # Search in open orders first, then pending new orders
        oinfo = self.open_limit_orders.get(order_id) or self.pending_new_orders.get(order_id)
        if oinfo is None:
            raise KeyError(f"order_id={order_id} not found in open or pending orders")
        if oinfo.order_type == OrderType.Market:
            raise ValueError("Market orders cannot be canceled")

        oinfo.timestamp = timestamp
        oinfo.src_type = src_type
        oinfo.src_timestamp = src_timestamp
        oinfo.src_id = src_id
        oinfo.misc = misc
        oinfo.order_status = OrderStatus.PendingCancel

        deadline = timestamp + pd.Timedelta(seconds=self.cancel_latency)
        self.pending_cancel_orders[order_id] = (oinfo, deadline)
        _snapshot = copy(oinfo)
        self.order_events.append(_snapshot)
        return _snapshot

    async def cancel_all_orders(self, timestamp: datetime.datetime, sym: str, side: int = None):
        targets = {}
        for oid, o in self.open_limit_orders.items():
            if (side is None or side == o.side) and o.order_status not in (OrderStatus.PendingCancel,):
                targets[oid] = o
        for oid, o in self.pending_new_orders.items():
            if o.order_type != OrderType.Market and (side is None or side == o.side):
                targets[oid] = o
        for oid in targets:
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
        oinfo = self.open_limit_orders.get(order_id) or self.pending_new_orders.get(order_id)
        if oinfo is None:
            raise KeyError(f"order_id={order_id} not found in open or pending orders")
        if oinfo.order_type == OrderType.Market:
            raise ValueError("Market orders cannot be amended")
        if oinfo.order_status in (OrderStatus.PendingCancel, OrderStatus.Canceled):
            raise ValueError(f"Cannot amend order with status={oinfo.order_status}")

        oinfo.timestamp = timestamp
        oinfo.src_type = src_type
        oinfo.src_timestamp = timestamp
        oinfo.src_id = src_id
        oinfo.misc = misc
        oinfo.order_status = OrderStatus.PendingUpdate

        deadline = timestamp + pd.Timedelta(seconds=self.amend_latency)
        self.pending_amend_orders[order_id] = (oinfo, deadline, price, amount)
        _snapshot = copy(oinfo)
        self.order_events.append(_snapshot)
        return _snapshot

    async def check_order(self, order_id: str) -> BacktestOrder:
        for orders in [self.open_limit_orders, self.open_market_orders, self.pending_new_orders]:
            oinfo = orders.get(order_id)
            if oinfo is not None:
                return oinfo
        return BacktestOrder.null_order()

    async def on_message(self, msg: Dict) -> None:
        dtype: str = msg.get("_data_type")

        if not dtype.startswith("BT_"):
            return

        msg_mct = msg.get("market_created_timestamp")
        msg_timestamp = msg.get("timestamp") or msg_mct

        # --- 1. New -> Sent (order_latency elapsed) ---
        sent_ids = []
        for oid, o in self.pending_new_orders.items():
            if o.order_status == OrderStatus.PendingCancel:
                continue  # cancel waiting: skip Sent transition, let cancel handler deal with it
            elapsed = (msg_timestamp - o.timestamp).total_seconds()
            if elapsed >= self.order_latency:
                o.order_status = OrderStatus.Sent
                o.market_created_timestamp = msg_mct  # set exchange timestamp at Sent
                # o.timestamp is NOT updated per spec
                _snapshot = copy(o)
                self.sent_order_message[oid] = _snapshot
                self.order_events.append(_snapshot)
                if o.order_type == OrderType.Market:
                    self.open_market_orders[oid] = o
                elif o.order_type == OrderType.Stop:
                    self.open_stop_orders[oid] = o
                else:
                    self.open_limit_orders[oid] = o
                await self.strategy.on_message(o.to_dict(to_string_timestamp=False))
                sent_ids.append(oid)
        for oid in sent_ids:
            del self.pending_new_orders[oid]

        # --- 2. PendingUpdate -> Updated (amend_latency elapsed) ---
        updated_ids = []
        for oid, (o, deadline, new_price, new_amount) in self.pending_amend_orders.items():
            if msg_timestamp >= deadline:
                if o.order_status == OrderStatus.Filled:
                    # Filled before amend completed: amend silently discarded
                    updated_ids.append(oid)
                    continue
                o.price = new_price
                o.amount = new_amount
                o.market_created_timestamp = msg_mct  # reset mct so fill judgment restarts
                o.timestamp = msg_timestamp
                o.order_status = OrderStatus.Updated
                _snapshot = copy(o)
                self.amend_order_message[oid] = _snapshot
                self.order_events.append(_snapshot)
                await self.strategy.on_message(o.to_dict(to_string_timestamp=False))
                updated_ids.append(oid)
        for oid in updated_ids:
            del self.pending_amend_orders[oid]

        # --- 3. PendingCancel -> Canceled (cancel_latency elapsed) ---
        canceled_ids = []
        for oid, (o, deadline) in self.pending_cancel_orders.items():
            if msg_timestamp >= deadline:
                if o.order_status == OrderStatus.Filled:
                    # Filled before cancel completed: cancel silently discarded
                    canceled_ids.append(oid)
                    continue
                o.order_status = OrderStatus.Canceled
                o.timestamp = msg_timestamp  # overwrite timestamp at cancel completion
                o.market_created_timestamp = msg_timestamp  # overwrite timestamp at cancel completion
                _snapshot = copy(o)
                self.cancel_order_message[oid] = _snapshot
                self.order_events.append(_snapshot)
                self.open_limit_orders.pop(oid, None)
                self.pending_new_orders.pop(oid, None)
                await self.strategy.on_message(o.to_dict(to_string_timestamp=False))
                canceled_ids.append(oid)
        for oid in canceled_ids:
            del self.pending_cancel_orders[oid]

        # --- 4. Clean up closed limit orders ---
        self.open_limit_orders = {
            oid: o for oid, o in self.open_limit_orders.items()
            if o.order_status not in (OrderStatus.Canceled, OrderStatus.Filled)
        }

        # --- 5. Fill judgment ---
        if dtype == "BT_Rate" and msg.get("venue") == self.venue:
            # Market orders: fill at best bid/ask
            sym = msg.get("sym")
            filled_ids = []
            for oid, o in self.open_market_orders.items():
                if o.sym == sym and pd.notna(o.market_created_timestamp) and msg_mct >= o.market_created_timestamp:
                    o.order_status = OrderStatus.Filled
                    o.price = msg["best_bid"] if o.side < 0 else msg["best_ask"]
                    o.executed_amount = o.amount
                    o.timestamp = o.market_created_timestamp = msg["market_created_timestamp"]
                    _snapshot = copy(o)
                    self.filled_orders_message[oid] = _snapshot
                    self.order_events.append(_snapshot)
                    t = Trade.from_order(
                        msg["market_created_timestamp"], msg["market_created_timestamp"],
                        price=o.price, trade_id=uuid4().hex, exec_amount=o.executed_amount, order=o,
                    )
                    self.update_position(t)
                    self.transactions.append(t)
                    await self.strategy.on_message(t.to_dict(to_string_timestamp=False))
                    await self.strategy.on_message(o.to_dict(to_string_timestamp=False))
                    filled_ids.append(oid)
            self.open_market_orders = {oid: o for oid, o in self.open_market_orders.items() if oid not in filled_ids}

        elif dtype == "BT_MarketTrade" and msg.get("venue") == self.venue:
            # Limit orders: fill against market trades
            sym = msg.get("sym")
            for oid, o in self.open_limit_orders.items():
                if o.sym != sym:
                    continue
                if pd.isna(o.market_created_timestamp) or msg_mct < o.market_created_timestamp:
                    continue
                if (o.side * msg["side"] < 0) and ((msg["price"] - o.price) * o.side <= 0):
                    filled_amount = 0
                    if msg["price"] == o.price:
                        # potentially filled: estimate based on book depth
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
                        _snapshot = copy(o)
                        self.filled_orders_message[oid] = _snapshot
                        self.order_events.append(_snapshot)
                        t: Trade = Trade.from_order(
                            msg["market_created_timestamp"], msg["market_created_timestamp"],
                            price=o.price, trade_id=uuid4().hex, exec_amount=filled_amount, order=o,
                        )
                        self.update_position(t)
                        self.transactions.append(t)
                        await self.strategy.on_message(o.to_dict(to_string_timestamp=False))
                        await self.strategy.on_message(t.to_dict(to_string_timestamp=False))

            self.open_limit_orders = {
                oid: o for oid, o in self.open_limit_orders.items()
                if o.order_status not in (OrderStatus.Canceled, OrderStatus.Filled)
            }

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
        """全状態遷移イベントを時系列順に返す。
        各行が1つの状態遷移に対応: New / Sent / PendingCancel / Canceled /
        PendingUpdate / Updated / Partial / Filled
        """
        if not self.order_events:
            return pd.DataFrame()
        o = pd.DataFrame(self.order_events)
        return o.sort_values(["timestamp", "order_id"]).reset_index(drop=True)

    @property
    def transactions_frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.transactions)
