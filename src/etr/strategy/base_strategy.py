import time, datetime
import numpy as np
import pandas as pd
from uuid import uuid4
from abc import ABC, abstractmethod
from typing import Union, List, Dict, Callable, Tuple, Optional
from etr.core.datamodel import Order, Trade
from etr.common.logger import LoggerFactory
from etr.config import Config
from pathlib import Path


class StrategyBase:

    def __init__(self, model_id: str, log_file=None):
        self.model_id = model_id
        self.process_id = uuid4().hex
        logger_name = self.model_id if log_file is None else log_file.split("/")[-1].split(".")[0]
        if log_file is not None:
            log_file = Path(Config.LOG_DIR).joinpath(log_file).as_posix()
        self.logger = LoggerFactory().get_logger(logger_name=logger_name, log_file=log_file)

    @abstractmethod
    def on_message(self, msg: Dict):
        """
        dtype = msg.get("_data_type")
        if dtype == "Rate":
            strategy.client.update_open_pnl(msg)

        if dtype == "MarketTrade":
            self.process_trade_message(msg)

        if dtype == "Order":
            self.process_order(msg)
        """
