import time, datetime
import numpy as np
import pandas as pd
from copy import deepcopy
from uuid import uuid4
from abc import ABC, abstractmethod
from typing import Union, List, Dict, Callable, Tuple, Optional
from etr.common.logger import LoggerFactory
from etr.core.api_client.base import ExchangeClientBase
from etr.core.api_client import CoincheckRestClient
from etr.core.datamodel import Order, Trade, OrderStatus, OrderType
from etr.strategy import StrategyBase
from etr.core.notification.discord import async_send_discord_webhook


class DUMMY(StrategyBase):
    def __init__(
        self,
        client: ExchangeClientBase,
        log_file=None,
    ):
        super().__init__(model_id="DUMMY", log_file=log_file)

    async def on_message(self, msg: Dict):
        self.logger.info(f"Message: {msg}")
