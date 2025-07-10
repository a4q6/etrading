from collections import deque
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import numpy as np
from etr.core.cep.cep_base import CEP


class EMARealTime(CEP):

    def __init__(
        self,
        alpha: float,
        sym: str,
        venue: str,
        cache_duration: int = 60 * 10,  # [sec]
        log_file=None,
    ):
        super().__init__(logger_name=__class__.__name__, log_file=log_file)
        self.logger.info("EMA realtime initialized: ({}, {}, alpha={})".format(sym, venue, alpha))
        self.alpha = alpha
        self.sym = sym
        self.venue = venue
        self.cache_duration = cache_duration  # in seconds

        self.prev_price: Optional[float] = None
        self.ema_price: Optional[float] = None
        self.ema_ret: Optional[float] = None
        self.ema_vol: Optional[float] = None
        self.ema_squared_return: Optional[float] = None
        self.latest_result: Dict[str, Optional[float]] = {"timestamp": None, "ema_ret": None, "ema_vol": None}
        self.buffer = deque()
        self._counter = 0

    async def on_message(self, msg: dict):
        dtype = msg.get("_data_type")
        if dtype == "Rate":
            if msg["sym"] == self.sym and msg["venue"] == self.venue:
                self.update(msg["mid_price"], msg["timestamp"])
        elif dtype == "MarketTrade":
            if msg["sym"] == self.sym and msg["venue"] == self.venue:
                self.update(msg["price"], msg["timestamp"])

    def update(self, price: float, timestamp: datetime) -> Dict[str, Optional[float]]:
        if self.prev_price is None:
            self.prev_price = price
        elif price > 0 and abs(price / self.prev_price - 1) < 0.1:  # [NOTE] 10% threshold to avoid data missing
            # log return
            r_t = np.log(price / self.prev_price) * 1e4
            # EMA[Price]
            self.ema_price = price if self.ema_price is None else self.alpha * price + (1 - self.alpha) * self.ema_price
            # EMA[Return]
            self.ema_ret = r_t if self.ema_ret is None else self.alpha * r_t + (1 - self.alpha) * self.ema_ret
            # EMA[Volatility]
            r2_t = r_t ** 2
            self.ema_squared_return = r2_t if self.ema_squared_return is None else self.alpha * r2_t + (1 - self.alpha) * self.ema_squared_return
            self.ema_vol = np.sqrt(self.ema_squared_return)

            self.latest_result = {
                "timestamp": timestamp,
                "sym": self.sym,
                "venue": self.venue,
                "ema_price": self.ema_price,
                "ema_ret": self.ema_ret,
                "ema_vol": self.ema_vol,
            }

            self.buffer.append(self.latest_result.copy())
            self._clean_buffer(now=timestamp)
            self.prev_price = price

            # logging
            self._counter += 1
            if self._counter == 10000:
                self.logger.info(f"EMA: {self.latest_result}")
                self._counter = 0

    def _clean_buffer(self, now: datetime):
        threshold = now - timedelta(seconds=self.cache_duration)
        while self.buffer and self.buffer[0]["timestamp"] <= threshold:
            self.buffer.popleft()

    @property
    def history(self) -> List[Dict[str, Any]]:
        return list(self.buffer)
