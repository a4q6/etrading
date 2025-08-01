from collections import deque
from datetime import datetime, timedelta
from typing import List, Dict, Any
from copy import deepcopy
from etr.core.cep.cep_base import CEP


class OHLCV(CEP):
    def __init__(
        self,
        venue: str,
        sym: str,
        interval: int,  # [sec]
        cache_duration: int = 60 * 60,  # [sec]
        price_field_name: str = "mid_price",
        source_type: str = "Rate",
        log_file=None,
    ):
        # assert price_field_name in ["mid_price", "price"]
        # assert source_type in ["Rate", "MarketTrade"]
        super().__init__(logger_name=__class__.__name__, log_file=log_file)
        self.logger.info("{} OHLC CEP initialized: ({}, {}, interval={}s, cache={}s)".format(source_type, sym, venue, interval, cache_duration))

        self.venue = venue
        self.sym = sym
        self.interval = interval
        self.cache_duration = cache_duration
        self.source_type = source_type
        self.price_field_name = price_field_name

        self.current_candle = None
        self.current_start = None
        self.latest_price = None
        self.buffer = deque()

    async def on_message(self, msg: dict):

        dtype = msg.get("_data_type")
        if dtype not in (self.source_type, "Heartbeat"):
            return

        dt = msg.get("timestamp")
        if msg.get("venue") == self.venue and msg.get("sym") == self.sym:
            self.latest_price = msg.get(self.price_field_name)

        if self.latest_price is not None:
            # initialize (executed once)
            if self.current_start is None:
                self.current_start = self._floor_time(dt)
                self._init_new_candle(self.latest_price)

            # create new candle if timestamp entered new time bar
            while dt >= self.current_start + timedelta(seconds=self.interval):
                self._finalize_candle()
                self.current_start += timedelta(seconds=self.interval)
                self._init_new_candle(self.latest_price)
                self._clean_buffer(now=dt)

            # extract target price
            price = msg.get(self.price_field_name)
            if msg.get("venue") != self.venue or msg.get("sym") != self.sym:
                price = None

            # update
            if price is not None:
                side = msg.get("side", 0)
                amt = msg.get("amount", 0)
                bv = amt * (side > 0)
                sv = amt * (side < 0)
                self._update_candle(price, buy_volume=bv, sell_volume=sv)

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
            "volume": 0,
            "buy_volume": 0,
            "sell_volume": 0,
        }

    def _update_candle(self, price, buy_volume=0, sell_volume=0):
        self.current_candle["high"] = max(self.current_candle["high"], price)
        self.current_candle["low"] = min(self.current_candle["low"], price)
        self.current_candle["close"] = price
        self.current_candle["buy_volume"] += buy_volume
        self.current_candle["sell_volume"] += sell_volume
        self.current_candle["volume"] = self.current_candle["sell_volume"] + self.current_candle["buy_volume"]

    def _finalize_candle(self):
        if self.current_candle:
            self.logger.info(f"Finalized OHLC: {self.current_candle}")
            self.buffer.append(deepcopy(self.current_candle))

    def _clean_buffer(self, now: datetime):
        """remove older candle（now = tz-aware datetime）"""
        threshold = now - timedelta(seconds=self.cache_duration)
        while self.buffer and self.buffer[0]["timestamp"] + timedelta(seconds=self.interval) <= threshold:
            self.buffer.popleft()

    @property
    def history(self) -> List[Dict[str, Any]]:
        return list(self.buffer)
