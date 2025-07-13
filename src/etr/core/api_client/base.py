import time, datetime
import numpy as np
import pandas as pd
import json
import itertools
import websocket
import logging
from pathlib import Path
from dataclasses import asdict, dataclass
from abc import ABC, abstractmethod
from typing import Union, List, Dict, Callable, Set, Optional

from etr.common.logger import LoggerFactory
from etr.strategy.base_strategy import StrategyBase
from etr.config import Config
from etr.core.async_logger import AsyncBufferedLogger
from etr.core.datamodel import Order, Rate, Trade


class ExchangeClientBase(ABC):

    def __init__(
        self,
        log_file: Optional[str] = "exchange.log",
        **kwargs,
    ):
        # logger
        tp_file = Path(Config.TP_DIR)
        self.ticker_plant = AsyncBufferedLogger(
            logger_name=f"TP-{self.__class__.__name__}-ALL",
            log_dir=tp_file.as_posix()
        )
        logger_name = "ex-client" if log_file is None else log_file.split("/")[-1].split(".")[0]
        if log_file is not None:
            log_file = Path(Config.LOG_DIR).joinpath(log_file).as_posix()
        self.logger = LoggerFactory().get_logger(logger_name=logger_name, log_file=log_file)
        self.closed_pnl = 0
        self.open_pnl = {}
        self.positions = {}  # {sym: (vwap, amount)}
        self.pending_positions = False
        self.strategy: StrategyBase = None

    def register_strategy(self, strategy: StrategyBase):
        self.strategy = strategy

    @abstractmethod
    async def send_order(
        self,
        timestamp: datetime.datetime,
        sym: str,
        side: int,
        price: float,
        amount: float,
        order_type: str,
        src_type: str,
        src_timestamp: datetime.datetime,
        src_id: str = None,
        **kwargs
    ) -> Order:
        pass

    @abstractmethod
    async def amend_order(
        self,
        timestamp: datetime.datetime,
        order_id: str,
        price: float,
        amount: float,
        order_type: str,
        src_type: str,
        src_timestamp: datetime.datetime,
        src_id: str = None,
        **kwargs
    ) -> Order:
        pass

    @abstractmethod
    async def cancel_order(
        self,
        order_id: str, 
        timestamp: datetime.datetime, 
        src_type: str,
        src_timestamp: datetime.datetime,
        src_id: str = None,
        **kwargs
    ) -> Order:
        pass

    @abstractmethod
    async def check_order(
        self,
        order_id: str,
    ) -> Order:
        pass

    def update_open_pnl(self, rate: Rate) -> None:
        if rate.sym in self.positions:
            ref_price = rate.mid_price
            self.open_pnl[rate.sym] = (ref_price - self.positions[rate.sym][0]) * self.positions[rate.sym][1]

    def update_position(self, trade: Trade) -> None:
        sym = trade.sym
        if not sym in self.positions or self.positions[sym][1] == 0:
            # New
            self.positions[sym] = [trade.price, trade.amount * trade.side]
        else:
            exec_amount = (trade.amount * trade.side)
            cur_size = self.positions[sym][1]
            cur_side = np.sign(cur_size)
            new_size = cur_size + exec_amount
            if round(new_size, 7) == 0:
                # close
                self.closed_pnl += cur_side * (trade.price - self.positions[sym][0]) * trade.amount
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
                self.closed_pnl += cur_side * (trade.price - self.positions[sym][0]) * trade.amount
                self.positions[sym][1] = new_size
            else:
                raise ValueError()
            
    def current_position(self, sym: str) -> float:
        amt = self.positions.get("sym", 0)
        if isinstance(amt, list):
            return amt[-1]
        else:
            return amt
