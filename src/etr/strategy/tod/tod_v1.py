import time, datetime
import numpy as np
import pandas as pd
from copy import deepcopy
from uuid import uuid4
from abc import ABC, abstractmethod
from typing import Union, List, Dict, Callable, Tuple, Optional
from etr.common.logger import LoggerFactory
from etr.core.api_client.base import ExchangeClientBase
from etr.core.datamodel import Order, Trade, OrderStatus, OrderType
from etr.strategy import StrategyBase


class TOD_v1(StrategyBase):
    """
    - Market Order Entry を (st -> et)で行う
    - SL/TPはRate監視でMOをトリガー
    - Feed:
        - 取引対象のRateのみ
    """
    def __init__(
        self,
        venue: str,
        client: ExchangeClientBase,
        entry_config = [
            {"start": datetime.time(2, 0), "holding_minutes": 60 * 8, "sym": "XRPJPY", "side": 1, "amount": 0.001, "spread_filter": 100, "tp_level": np.nan, "sl_level": 150},
        ],
        model_id="TOD_v1",
        log_file=None,
    ):
        super().__init__(model_id, log_file)
        self.venue = venue
        self.client = client
        self.entry_config = entry_config
        self.logger.info(f"Starting up {self.model_id} strategy on {self.venue}. \n{self.entry_config}")

    @property
    def cur_pos(self):
        return self.client.positions.get(self.sym, [0, 0])[1]  # [vwap, amount]

    @staticmethod
    def next_occurrence(given_dt: datetime.datetime, target_time: datetime.time) -> datetime.datetime:
        candidate_dt = datetime.datetime.combine(given_dt.date(), target_time).replace(tzinfo=datetime.timezone.utc)
        if candidate_dt < given_dt:
            candidate_dt += datetime.timedelta(days=1)
        return candidate_dt
    
    def warmup(self, now=datetime.datetime.now(tz=datetime.timezone.utc)):
        for config in self.entry_config:
            config["next_entry_time"] = self.next_occurrence(given_dt=now, target_time=config["start"])
            config["next_exit_time"] = config["next_entry_time"] + datetime.timedelta(minutes=config["holding_minutes"])
            config["entry_order"] = None
            config["exit_order"] = None
        return 

    async def on_message(self, msg: Dict):
        if "_data_type" not in msg:
            return
        dtype = msg.get("_data_type")

        if dtype == "Trade":
            for config in self.entry_config:
                if isinstance(config["exit_order"], Order) and config["exit_order"].order_id == msg["order_id"]:
                    # when exit order filled
                    config["next_entry_time"] = self.next_occurrence(given_dt=msg["timestamp"], target_time=config["start"])
                    config["next_exit_time"] = config["next_entry_time"] + datetime.timedelta(minutes=config["holding_minutes"])
                    config["entry_order"] = None
                    config["exit_order"] = None
                    self.logger.info(f"Exit MO (orderID = '{msg['order_id']}') has filled. Refreshed config for next entry. \n{config}")

        # update order status
        if dtype == "Order":
            for config in self.entry_config:
                if isinstance(config["entry_order"], Order) and config["entry_order"].universal_id == msg["universal_id"]:
                    msg.pop("_data_type")
                    config["entry_order"] = Order(**msg)
                    self.logger.info(f"Update entry order status (orderID = '{msg['order_id']}') \n{config}")
                if isinstance(config["exit_order"], Order) and config["exit_order"].universal_id == msg["universal_id"]:
                    msg.pop("_data_type")
                    config["exit_order"] = Order(**msg)
                    self.logger.info(f"Update exit order status (orderID = '{msg['order_id']}') \n{config}")

        # main logic
        if dtype == "Rate":
            if msg["venue"] == self.venue:
                sym = msg.get("sym")
                for config in self.entry_config:
                    if config["next_exit_time"] + datetime.timedelta(minutes=10) < msg["timestamp"]:
                        config["next_entry_time"] = self.next_occurrence(given_dt=msg["timestamp"], target_time=config["start"])
                        config["next_exit_time"] = config["next_entry_time"] + datetime.timedelta(minutes=config["holding_minutes"])
                    if config["sym"] != sym:
                        continue

                    misc = f"{config['start']}+{config['holding_minutes']}M"
                    # entry
                    if (config["entry_order"] is None) and (config["next_entry_time"] < msg["timestamp"] < config["next_entry_time"] + datetime.timedelta(minutes=5)):
                        if (msg["best_ask"] / msg["best_bid"] - 1) * 1e4 < config["spread_filter"]:
                            self.logger.info(f"Try sending MO for entry {misc} ...")
                            config["entry_order"] = "sending"
                            config["entry_order"] = await self.client.send_order(
                                timestamp=msg["timestamp"], sym=config["sym"], side=config["side"], price=msg["mid_price"], amount=config["amount"],
                                order_type=OrderType.Market, src_type=dtype, src_id=msg["universal_id"], src_timestamp=msg["timestamp"], misc=f"entry {misc}"
                            )

                    # SL
                    if (config["entry_order"] is not None) and (config["exit_order"] is None):
                        if (msg["mid_price"] / config["entry_order"].price - 1) * 1e4 * config["side"] < -config["sl_level"]:
                            self.logger.info(f"Try sending MO for SL {misc} ...")
                            config["exit_order"] = "sending"
                            config["exit_order"] = await self.client.send_order(
                                timestamp=msg["timestamp"], sym=config["sym"], side=-1 * config["side"], price=msg["mid_price"], amount=config["entry_order"].executed_amount,
                                order_type=OrderType.Market, src_type=dtype, src_id=msg["universal_id"], src_timestamp=msg["timestamp"], misc=f"sl {misc}"
                            )

                    # exit
                    if (config["entry_order"] is not None) and (config["exit_order"] is None):
                        if config["next_exit_time"] < msg["timestamp"]:
                            self.logger.info(f"Try sending MO for exit {misc} ...")
                            config["exit_order"] = "sending"
                            config["exit_order"] = await self.client.send_order(
                                timestamp=msg["timestamp"], sym=config["sym"], side=-1 * config["side"], price=msg["mid_price"], amount=config["entry_order"].executed_amount,
                                order_type=OrderType.Market, src_type=dtype, src_id=msg["universal_id"], src_timestamp=msg["timestamp"], misc=f"exit {misc}"
                            )

