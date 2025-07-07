from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any
from copy import deepcopy
from etr.config import Config
from etr.common.logger import LoggerFactory


class RateOHLC:
    def __init__(
        self,
        venue: str,
        sym: str,
        interval: int,
        cache_duration: int = 60 * 60,
        log_file=None,
    ):
        # logger
        if log_file is not None:
            log_file = Path(Config.LOG_DIR).joinpath(log_file).as_posix()
        logger_name = __class__.__name__
        self.logger = LoggerFactory().get_logger(logger_name=logger_name, log_file=log_file)

        self.venue = venue
        self.sym = sym
        self.interval = interval  # [sec]
        self.cache_duration = cache_duration  # [sec]

        self.current_candle = None
        self.current_start = None
        self.latest_price = None
        self.buffer = deque()

    async def on_message(self, msg: dict):

        dtype = msg.get("_data_type")
        if dtype not in ("Rate", "Heartbeat"):
            return

        dt = msg.get("timestamp")
        if msg.get("venue") == self.venue and msg.get("sym") == self.sym:
            price = msg.get("mid_price")
            self.latest_price = price if price is not None else None

        if self.latest_price is not None:
            # initialize
            if self.current_start is None:
                self.current_start = self._floor_time(dt)
                self._init_new_candle(price)

            # create new candle if timestamp entered new time bar
            while dt >= self.current_start + timedelta(seconds=self.interval):
                self._finalize_candle()
                self.current_start += timedelta(seconds=self.interval)
                self._init_new_candle(price)
                self._clean_buffer(now=dt)

            # update
            if price is not None:
                self._update_candle(price)

    def _floor_time(self, dt: datetime) -> datetime:
        total_seconds = int(dt.timestamp())
        floored = total_seconds - (total_seconds % self.interval)
        return datetime.fromtimestamp(floored, tz=dt.tzinfo)

    def _init_new_candle(self, price):
        self.current_candle = {
            "timestamp": self.current_start,
            "venue": self.venue,
            "sym": self.sym,
            "open": self.latest_price,
            "high": self.latest_price,
            "low": self.latest_price,
            "close": self.latest_price,
        }

    def _update_candle(self, price):
        self.current_candle["high"] = max(self.current_candle["high"], price)
        self.current_candle["low"] = min(self.current_candle["low"], price)
        self.current_candle["close"] = price

    def _finalize_candle(self):
        if self.current_candle:
            self.logger.info("Finalized OHLC:", self.current_candle)
            self.buffer.append(deepcopy(self.current_candle))

    def _clean_buffer(self, now: datetime):
        """remove older candle（nowはtz-aware datetime）"""
        threshold = now - timedelta(seconds=self.cache_duration)
        while self.buffer and self.buffer[0]["timestamp"] + timedelta(seconds=self.interval) <= threshold:
            self.buffer.popleft()

    @property
    def ohlc(self) -> List[Dict[str, Any]]:
        return list(self.buffer)
