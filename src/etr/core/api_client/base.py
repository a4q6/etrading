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
from etr.config import Config
from etr.core.async_logger import AsyncBufferedLogger
from etr.core.api_client.api_counter import ApiCounter
from etr.core.datamodel import Order, Rate, Trade


class ExchangeClientBase(ABC):

    def __init__(
        self,
        api_limit: int, 
        api_count_period_sec: float,
        log_name: Optional[str] = "exchange.log",
        **kwargs,
    ):
        # api counter
        self.api_counter = ApiCounter(api_limit, api_count_period_sec)
        # logger
        tp_file = Path(Config.TP_DIR)
        self.ticker_plant = AsyncBufferedLogger(
            logger_name=f"TP-{self.__class__.__name__}-ALLSYM",
            log_dir=tp_file.as_posix()
        )
        log_file = Path(Config.LOG_DIR).joinpath(f"{log_name}").as_posix()
        self.logger = LoggerFactory().get_logger(logger_name="main", log_file=None if log_name is None else log_file)

        self.open_limit_orders: Dict[str, Order] = {}
        self.open_stop_orders: Dict[str, Order] = {}
        self.open_market_orders: Dict[str, Order] = {}
        self.closed_orders = {}
        self.closed_pnl = 0
        self.open_pnl = {}
        self.positions = {}  # {sym: (amount, vwap)}
        self.transactions: List[Trade] = []

    @abstractmethod
    async def send_order(
        self,
        strategy,
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
        strategy,
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
        strategy, 
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

    @abstractmethod
    async def update_open_pnl(self, rate: Rate):
        pass
