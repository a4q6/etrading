from .base_strategy import StrategyBase
import time, datetime
import numpy as np
import pandas as pd
from copy import deepcopy
from uuid import uuid4
from abc import ABC, abstractmethod
from typing import Union, List, Dict, Callable, Tuple, Optional
from etr.common.logger import LoggerFactory
from etr.core.api_client.base import ExchangeClientBase
from etr.core.datamodel import Order, OrderStatus, OrderType


class Sample(StrategyBase):
    def __init__(
        self,
        sym: str,
        client: ExchangeClientBase,
        side = +1,
        model_id="Sample",
        log_file=None
    ):
        super().__init__(model_id, log_file)
        self.sym = sym
        self.side = side
        self.client = client
        self.entry_order = Order.null_order()
        self.exit_order = Order.null_order()
        self.last_entry = pd.NaT

    @property
    def cur_pos(self):
        return self.client.positions.get(self.sym, [0, 0])[1]

    async def on_message(self, msg: Dict):
        dtype = msg.get("_data_type")

        if dtype == "Trade":
            self.entry_order = deepcopy(self.entry_order)
            self.entry_order.timestamp = msg["timestamp"]
            self.entry_order.order_status = OrderStatus.Filled

        if dtype == "Rate":
            if self.cur_pos == 0:
                if (self.entry_order.timestamp) + datetime.timedelta(seconds=5) < msg["timestamp"]:
                    if not self.entry_order.order_status in [OrderStatus.Filled, OrderStatus.Canceled]:
                        self.entry_order = await self.client.cancel_order(self, self.entry_order.order_id, timestamp=msg["timestamp"], src_type=dtype, src_timestamp=msg["timestamp"], src_id=msg["universal_id"])
                    price = msg["best_bid"] if self.side > 0 else msg["best_ask"]
                    self.entry_order = await self.client.send_order(
                        self, msg["timestamp"], sym=self.sym, side=self.side, amount=0.01, price=price * (1 - self.side * 1/1e4), order_type=OrderType.Limit,
                        src_type=dtype, src_timestamp=msg["timestamp"], src_id=msg["universal_id"],
                    )

        if dtype is not None and not dtype.startswith("BT_") and self.cur_pos != 0:
            if msg["timestamp"] > self.entry_order.timestamp + datetime.timedelta(minutes=10):
                o = await self.client.send_order(
                    self, msg["timestamp"], sym=self.sym, side=-1 * self.side, amount=0.01, price=None, order_type=OrderType.Market,
                    src_type=dtype, src_timestamp=msg["timestamp"], src_id=msg["universal_id"],
                )
                self.entry_order.timestamp = o.timestamp
