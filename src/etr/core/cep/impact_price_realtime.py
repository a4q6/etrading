import numpy as np
import numba
from collections import deque
from datetime import datetime, timedelta
from typing import Dict, Any, List
from etr.core.cep.cep_base import CEP


class ImpactPrice(CEP):

    def __init__(
        self,
        target_amount: float,
        sym: str,
        venue: str,
        use_term_amount: bool = True,
        cache_duration: int = 10,  # [sec]
        log_file=None,
    ):
        super().__init__(logger_name=__class__.__name__, log_file=log_file)
        self.logger.info("ImpactPrice CEP initialized: ({}, {}, {})".format(sym, venue, target_amount))
        self.sym = sym
        self.venue = venue
        self.target_amount = target_amount
        self.use_term_amount = use_term_amount
        self.cache_duration = cache_duration

        # features
        self.mid_price = np.nan
        self.impact_bid_price = np.nan
        self.impact_ask_price = np.nan
        self.impact_bid_spread = np.nan
        self.impact_ask_spread = np.nan
        self.impact_spread = np.nan
        self.spread_imbalance = np.nan
        self.latest_result = {}
        self.buffer = deque()
        self._counter = 0

    async def on_message(self, msg: Dict[str, Any]):
        dtype = msg.get("_data_type")
        if dtype != "MarketBook":
            return

        if msg.get("sym") == self.sym and msg.get("venue") == self.venue:
            bids = msg["bids"]
            asks = msg["asks"]

            if len(bids) < 3 or len(asks) < 3:
                return  # skip if bids/asks doesn't have enoough ladders

            bids = np.vstack(msg["bids"])
            asks = np.vstack(msg["asks"])
            mid_price = (bids[0][0] + asks[0][0]) / 2
            if np.isnan(self.mid_price) or abs(mid_price / self.mid_price - 1) < 0.1:
                self.mid_price = mid_price
                self.impact_bid_price = self._calc_impact_price(bids, target_amount=self.target_amount, use_term=self.use_term_amount)
                self.impact_ask_price = self._calc_impact_price(asks, target_amount=self.target_amount, use_term=self.use_term_amount)
                self.impact_bid_spread = abs(self.impact_bid_price / self.mid_price - 1) * 1e4
                self.impact_ask_spread = abs(self.impact_ask_price / self.mid_price - 1) * 1e4
                self.impact_spread = (self.impact_ask_spread + self.impact_bid_spread)
                self.spread_imbalance = self.impact_ask_spread - self.impact_bid_spread
                self.latest_result = {
                    "timestamp": msg["timestamp"],
                    "sym": self.sym,
                    "venue": self.venue,
                    "mid_price": self.mid_price,
                    "impact_bid_price": self.impact_bid_price,
                    "impact_ask_price": self.impact_ask_price,
                    "impact_bid_spread": self.impact_bid_spread,
                    "impact_ask_spread": self.impact_ask_spread,
                    "impact_spread": self.impact_spread,
                    "spread_imbalance": self.spread_imbalance,
                }
                self.buffer.append(self.latest_result)
                self._clean_buffer(msg["timestamp"])

                self._counter += 1
                if self._counter == 3000:
                    self.logger.info(f"ImpactPrice: {self.latest_result}")
                    self._counter = 0

    def _clean_buffer(self, now: datetime):
        threshold = now - timedelta(seconds=self.cache_duration)
        while self.buffer and self.buffer[0]["timestamp"] <= threshold:
            self.buffer.popleft()

    @staticmethod
    @numba.njit
    def _calc_impact_price(array: np.ndarray, target_amount=5e-3, use_term=False, force_return=False) -> float:
        N = len(array)
        prices = np.empty(N)
        weights = np.empty(N)
        cum_amount = 0.0
        idx = 0
        fill_target_amount = False
        for i in range(N):
            price = array[i, 0]
            qty = array[i, 1]
            delta_amount = price * qty if use_term else qty
            cum_amount += delta_amount
            if cum_amount >= target_amount:
                weights[idx] = target_amount - (cum_amount - delta_amount)
                prices[idx] = price
                idx += 1
                fill_target_amount = True
                break
            else:
                prices[idx] = price
                weights[idx] = delta_amount
                idx += 1

        # null handling
        if idx == 0:
            return np.nan
        if (not fill_target_amount) and (not force_return):
            return np.nan

        return np.average(prices[:idx], weights=weights[:idx])

    @property
    def history(self) -> List[Dict[str, Any]]:
        return list(self.buffer)
