import time, datetime
import numpy as np
import pandas as pd
import pytz
from uuid import uuid4
from typing import Union, List, Dict, Callable, Tuple, Optional
from collections import deque
from etr.core.api_client.base import ExchangeClientBase
from etr.core.datamodel import Order, OrderStatus, OrderType, BacktestOrder
from etr.strategy import StrategyBase
from etr.data.data_loader import load_data


class GmoMMv1(StrategyBase):
    """
    GMO BTCJPY Cross-Venue Adaptive Market Making Strategy

    Combines:
    1. Cross-venue reference price (GlobalMid) and deviation
    2. Dynamic spread based on volatility + adverse selection
    3. Inventory management with position skew
    4. Adverse selection defense using cross-venue momentum

    FairValue(t) = GlobalMid(t) * (1 + EMA[Deviation(t)])
    DynamicSpread = clip(BaseSpread + VolSpread + AdverseSpread, min, max)
    BidPrice = FairValue * (1 - (Spread - Bias) / 1e4)
    AskPrice = FairValue * (1 + (Spread + Bias) / 1e4)
    """
    def __init__(
        self,
        sym: str,
        venue: str,
        amount: float,
        client: ExchangeClientBase,
        # Spread parameters
        base_offset: float = 2.0,       # base spread (bps)
        vol_coef: float = 0.5,          # volatility coefficient
        adverse_coef: float = 1.0,      # adverse selection coefficient
        min_spread: float = 1.0,        # min spread (bps)
        max_spread: float = 10.0,       # max spread (bps)
        # Inventory management
        skew_coef: float = 2.0,         # position skew coefficient
        max_position: float = 0.005,     # max position (BTC)
        # Time management
        position_horizon: float = 120,  # max holding time (seconds)
        # Adverse selection defense
        adverse_threshold: float = 3.0,  # threshold (bps) for adverse selection defense
        # Signal weights
        w_tob: float = 0.3,            # TOB imbalance weight
        w_xvenue: float = 0.5,         # cross-venue momentum weight
        w_flow: float = 0.2,           # trade flow weight
        # Reference markets
        reference: List[Tuple[str, str]] = None,
        # Volatility filter
        vol_threshold: float = 100,     # annualized vol threshold (%/Y) to stop trading
        # Volatility adaptation
        vol_baseline: float = 5.0,          # "normal" realized_vol_5min level
        adverse_adapt_power: float = 0.0,   # adverse_threshold scaling exponent (0=disabled)
        horizon_adapt_power: float = 0.0,   # position_horizon scaling exponent (0=disabled)
        offset_adapt_power: float = 0.0,    # base_offset scaling exponent (0=disabled)
        max_horizon: float = 600,           # position_horizon upper bound (seconds)
        # Misc
        dev_window: datetime.timedelta = datetime.timedelta(minutes=5),
        decimal: int = 0,
        model_id: str = "GmoMM_v1",
        log_file: str = None,
    ):
        super().__init__(model_id, log_file)
        self.sym = sym
        self.venue = venue
        self.client = client
        self.amount = amount

        # Spread params
        self.base_offset = base_offset
        self.vol_coef = vol_coef
        self.adverse_coef = adverse_coef
        self.min_spread = min_spread
        self.max_spread = max_spread

        # Inventory
        self.skew_coef = skew_coef
        self.max_position = max_position

        # Time management
        self.horizon = position_horizon

        # Adverse selection
        self.adverse_threshold = adverse_threshold

        # Signal weights
        self.w_tob = w_tob
        self.w_xvenue = w_xvenue
        self.w_flow = w_flow

        # Reference markets
        if reference is None:
            reference = [
                ('bitflyer', 'FXBTCJPY'),
                ('binance', 'BTCUSDT'),
                ('bitmex', 'XBTUSD'),
            ]
        self.reference = reference

        # Vol filter
        self.vol_threshold = vol_threshold

        # Volatility adaptation
        self.vol_baseline = vol_baseline
        self.adverse_adapt_power = adverse_adapt_power
        self.horizon_adapt_power = horizon_adapt_power
        self.offset_adapt_power = offset_adapt_power
        self.max_horizon = max_horizon

        # Misc
        self.dev_window = dev_window
        self.decimal = decimal

        # === State variables ===
        # Orders
        self.bid_order: Order = BacktestOrder.null_order()
        self.ask_order: Order = BacktestOrder.null_order()
        self.exit_order: Order = BacktestOrder.null_order()

        # Price state
        self.usdjpy: float = np.nan
        self.mid: float = np.nan  # target venue's mid
        self.latest_rate: Dict = {}
        self.latest_mid: Dict[Tuple[str, str], float] = {k: np.nan for k in reference}
        self.latest_gmid: float = np.nan

        # Deviation tracking
        self.dev_hist: deque = deque()
        self._dev_total = 0.0
        self._dev_count = 0
        self.dev_ma: float = np.nan

        # Volatility tracking (1sec returns for 5min window)
        self.gmid_hist: deque = deque()
        self.last_update_gmid_hist = datetime.datetime(2000, 1, 1, tzinfo=pytz.timezone("UTC"))
        self.gmid_std: float = np.nan
        self._vol_cache: List[float] = []

        # Cross-venue momentum
        self._ref_ret_hist: Dict[Tuple[str, str], deque] = {k: deque(maxlen=10) for k in reference}
        self._last_ref_mid: Dict[Tuple[str, str], float] = {k: np.nan for k in reference}

        # TOB imbalance from MarketBook
        self.tob_imbalance: float = 0.0

        # Trade flow
        self._trade_flow_hist: deque = deque(maxlen=100)
        self.trade_flow_5s: float = 0.0

        # Realized vol (5min, bps/s)
        self._mid_ret_hist: deque = deque(maxlen=300)
        self.realized_vol_5min: float = 5.0  # default

        # Position entry time
        self._entry_time: Optional[datetime.datetime] = None
        self._last_executed_at = pd.Timestamp("2000-01-01", tz="UTC")

        # Logging
        self._log_timestamp = datetime.datetime.now()

    def warmup(self, now=datetime.datetime.now(datetime.timezone.utc)):
        """Load historical rate data to initialize deviation and vol estimates."""
        tkynow = pd.Timestamp(now).astimezone(pytz.timezone("Asia/Tokyo"))
        if tkynow.hour in [10]:
            raise RuntimeError("Do not turn on strategy during EoD process.")

        self.logger.info("Warming up GmoMMv1 strategy. Loading latest rates...")
        rates = pd.concat(
            [load_data(date=now.date(), table="Rate", venue=venue, symbol=symbol)
             for venue, symbol in self.reference
            ] + [load_data(date=now.date(), table="Rate", venue="gmo", symbol="USDJPY")
            ] + [load_data(date=now.date(), table="Rate", venue=self.venue, symbol=self.sym)]
        )
        rates = rates.loc[
            (now - self.dev_window * 2 < rates.timestamp) & (rates.timestamp <= now)
        ].sort_values("timestamp").assign(_data_type="Rate")
        self.logger.info(f"Loaded rate data {rates.timestamp.min()} - {rates.timestamp.max()}")
        rates_msg = rates.to_dict(orient="records")
        for msg in rates_msg:
            self._update_deviation(msg)
        self.logger.info("Warmup done.")

    @property
    def _vol_ratio(self) -> float:
        """Ratio of current realized vol to baseline (>=1.0)."""
        return max(self.realized_vol_5min, self.vol_baseline) / self.vol_baseline

    @property
    def cur_pos(self):
        return self.client.positions.get(self.sym, [0, 0])[1]

    def my_ceil(self, num: float) -> float:
        factor = 10 ** self.decimal
        return np.ceil(num * factor) / factor

    def my_floor(self, num: float) -> float:
        factor = 10 ** self.decimal
        return np.floor(num * factor) / factor

    async def on_message(self, msg: Dict):
        dtype = msg.get("_data_type")

        # heartbeat logging
        if self._log_timestamp + datetime.timedelta(seconds=60) < datetime.datetime.now():
            self._log_timestamp = datetime.datetime.now()
            vr = self._vol_ratio
            self.logger.info(
                f"heartbeat pos={self.cur_pos:.4f} dev_ma={self.dev_ma:.2f} "
                f"vol={self.realized_vol_5min:.2f} vol_ratio={vr:.2f} "
                f"eff_adverse={self.adverse_threshold * (vr ** self.adverse_adapt_power):.2f} "
                f"eff_horizon={min(self.horizon * (vr ** self.horizon_adapt_power), self.max_horizon):.0f} "
                f"eff_base={self.base_offset * (vr ** self.offset_adapt_power):.2f}"
            )

        if dtype in ("Order", "BacktestOrder"):
            await self._handle_order(msg)

        elif dtype == "Rate":
            self._update_deviation(msg)
            if msg.get("venue") == self.venue and msg.get("sym") == self.sym:
                await self._update_quotes(msg)

        elif dtype == "BT_Rate":
            if msg.get("venue") == self.venue and msg.get("sym") == self.sym:
                self.mid = msg.get("mid_price", (msg.get("best_bid", 0) + msg.get("best_ask", 0)) / 2)
                self.latest_rate = msg

        elif dtype == "BT_MarketBook" or dtype == "MarketBook":
            self._update_tob_imbalance(msg)

        elif dtype == "BT_MarketTrade" or dtype == "MarketTrade":
            if msg.get("venue") == self.venue and msg.get("sym") == self.sym:
                self._update_trade_flow(msg)

        elif dtype == "Trade":
            self._handle_trade(msg)

    async def _handle_order(self, msg: Dict):
        oid = msg.get("order_id")
        if oid == self.bid_order.order_id:
            self.bid_order = Order(**{k: v for k, v in msg.items() if k != "_data_type"})
            if self.bid_order.order_status == OrderStatus.Filled:
                self.logger.info(f"BID FILLED @{self.bid_order.price}")
                self._update_entry_time(msg.get("timestamp"))

        elif oid == self.ask_order.order_id:
            self.ask_order = Order(**{k: v for k, v in msg.items() if k != "_data_type"})
            if self.ask_order.order_status == OrderStatus.Filled:
                self.logger.info(f"ASK FILLED @{self.ask_order.price}")
                self._update_entry_time(msg.get("timestamp"))

        elif oid == self.exit_order.order_id:
            self.exit_order = Order(**{k: v for k, v in msg.items() if k != "_data_type"})
            if self.exit_order.order_status == OrderStatus.Filled:
                self.logger.info("EXIT FILLED")
                self._entry_time = None

    def _update_entry_time(self, ts):
        """Set _entry_time only on first entry (flat -> position). Reset when flat."""
        if self.cur_pos == 0:
            self._entry_time = None
        elif self._entry_time is None:
            self._entry_time = ts

    def _handle_trade(self, msg: Dict):
        pass

    def _update_tob_imbalance(self, msg: Dict):
        """Extract TOB imbalance from MarketBook (handles numpy arrays, lists, tuples)."""
        bids = msg.get("bids", [])
        asks = msg.get("asks", [])
        try:
            if len(bids) > 0 and len(asks) > 0:
                bid_size = float(bids[0][1])
                ask_size = float(asks[0][1])
                total = bid_size + ask_size
                if total > 0:
                    self.tob_imbalance = (bid_size - ask_size) / total
        except (IndexError, TypeError, ValueError):
            pass

    def _update_trade_flow(self, msg: Dict):
        """Track trade flow imbalance over 5s window."""
        ts = msg.get("market_created_timestamp", msg.get("timestamp"))
        side = msg.get("side", 0)
        amount = msg.get("amount", 0)
        self._trade_flow_hist.append((ts, side * amount, amount))

        # calculate 5s flow
        if isinstance(ts, datetime.datetime):
            cutoff = ts - datetime.timedelta(seconds=5)
            signed_sum = 0
            volume_sum = 0
            for t, sa, a in self._trade_flow_hist:
                if t >= cutoff:
                    signed_sum += sa
                    volume_sum += a
            self.trade_flow_5s = signed_sum / volume_sum if volume_sum > 0 else 0

    def _update_deviation(self, msg: Dict):
        """Update cross-venue deviation and volatility estimates."""
        venue = msg.get("venue")
        sym = msg.get("sym")

        if sym == "USDJPY":
            mid = msg.get("mid_price")
            if mid is None:
                mid = (msg.get("best_bid", 0) + msg.get("best_ask", 0)) / 2
            self.usdjpy = mid
            return

        if venue == self.venue and sym == self.sym:
            mid = msg.get("mid_price")
            if mid is None:
                mid = (msg.get("best_bid", 0) + msg.get("best_ask", 0)) / 2
            # track mid returns for realized vol
            if not np.isnan(self.mid) and self.mid > 0 and mid > 0:
                ret = (mid / self.mid - 1) * 1e4
                self._mid_ret_hist.append(ret)
                if len(self._mid_ret_hist) >= 10:
                    self.realized_vol_5min = np.std(list(self._mid_ret_hist))
            self.mid = mid
            self.latest_rate = msg
            return

        ref_key = (venue, sym)
        if ref_key in self.latest_mid:
            mid = msg.get("mid_price")
            if mid is None:
                mid = (msg.get("best_bid", 0) + msg.get("best_ask", 0)) / 2

            # USD→JPY conversion
            if "USD" in sym and not np.isnan(self.usdjpy):
                jpy_mid = mid * self.usdjpy
            else:
                jpy_mid = mid

            # track cross-venue returns for momentum
            prev_mid = self._last_ref_mid.get(ref_key, np.nan)
            if not np.isnan(prev_mid) and prev_mid > 0:
                ret = (jpy_mid / prev_mid - 1) * 1e4
                self._ref_ret_hist[ref_key].append(ret)
            self._last_ref_mid[ref_key] = jpy_mid
            self.latest_mid[ref_key] = jpy_mid

            # Compute GlobalMid + Deviation
            if not np.isnan(self.mid) and all(not np.isnan(v) for v in self.latest_mid.values()):
                gmid = np.mean(list(self.latest_mid.values()))
                self.latest_gmid = gmid
                dev = (self.mid / gmid - 1) * 1e4
                ts = msg.get("timestamp")

                self.dev_hist.append([ts, dev])
                self._dev_total += dev
                self._dev_count += 1
                self._trim_dev(ts)
                self.dev_ma = self._dev_total / self._dev_count if self._dev_count > 0 else 0

                # gmid history for vol calculation (10s interval)
                if self.last_update_gmid_hist + datetime.timedelta(seconds=10) < ts:
                    self.last_update_gmid_hist = ts
                    self.gmid_hist.append([ts, gmid])
                    self._trim_gmid(ts)

    def _trim_dev(self, now: datetime.datetime):
        while self.dev_hist and (now - self.dev_hist[0][0]) > self.dev_window:
            _, old_val = self.dev_hist.popleft()
            self._dev_total -= old_val
            self._dev_count -= 1

    def _trim_gmid(self, now: datetime.datetime):
        while self.gmid_hist and (now - self.gmid_hist[0][0]) > self.dev_window:
            self.gmid_hist.popleft()

    def _calc_xvenue_momentum(self) -> float:
        """Cross-venue momentum: average of 5s cumulative returns across reference markets."""
        momentums = []
        for key in self.reference:
            hist = self._ref_ret_hist.get(key, deque())
            if len(hist) >= 2:
                # sum last 5 entries (roughly 5s for 1s updates)
                recent = list(hist)[-5:]
                momentums.append(sum(recent))
        return np.mean(momentums) if momentums else 0.0

    def _calc_direction_bias(self) -> float:
        """Combined directional signal."""
        xvenue_mom = self._calc_xvenue_momentum()
        bias = (
            self.w_tob * self.tob_imbalance +
            self.w_xvenue * xvenue_mom +
            self.w_flow * self.trade_flow_5s
        )
        return bias

    def _calc_dynamic_spread(self) -> float:
        """Volatility-adaptive spread."""
        effective_base = self.base_offset * (self._vol_ratio ** self.offset_adapt_power)
        vol_spread = self.realized_vol_5min * self.vol_coef
        xvenue_mom = abs(self._calc_xvenue_momentum())
        adverse_spread = xvenue_mom * self.adverse_coef
        spread = effective_base + vol_spread + adverse_spread
        return np.clip(spread, self.min_spread, self.max_spread)

    def _calc_fair_value(self) -> float:
        """Fair value based on GlobalMid and deviation EMA."""
        if np.isnan(self.latest_gmid) or np.isnan(self.dev_ma):
            return self.mid
        return self.latest_gmid * (1 + self.dev_ma / 1e4)

    def _is_itayose_time(self, ts) -> bool:
        """BitFlyer itayose filter (19:05-19:15 JST = 10:05-10:15 UTC)."""
        if hasattr(ts, 'hour'):
            if ts.hour == 10 and 5 <= ts.minute <= 15:
                return True
        return False

    async def _update_quotes(self, msg: Dict):
        """Main quoting logic: calculate fair value, spread, and manage orders."""
        if np.isnan(self.mid) or np.isnan(self.latest_gmid):
            return
        if len(self.dev_hist) < 10:
            return

        ts = msg.get("timestamp")

        # Itayose filter
        if self._is_itayose_time(ts):
            await self._cancel_all_quotes(msg)
            return

        # Annualized vol check
        if len(self.gmid_hist) > 10:
            gmid_arr = np.array(self.gmid_hist)[:, 1].astype(float)
            gmid_vol = np.std(np.diff(gmid_arr) / gmid_arr[:-1]) * 100 * np.sqrt(6 * 1440 * 365)
            if gmid_vol > self.vol_threshold:
                await self._cancel_all_quotes(msg)
                return

        cur_pos = self.cur_pos

        # === Position horizon check (vol-adaptive) ===
        effective_horizon = min(self.horizon * (self._vol_ratio ** self.horizon_adapt_power), self.max_horizon)
        if cur_pos != 0 and self._entry_time is not None:
            holding_time = (ts - self._entry_time).total_seconds()
            if holding_time > effective_horizon:
                self.logger.info(f"Position horizon exceeded ({holding_time:.0f}s > {effective_horizon:.0f}s), force close")
                await self._force_close(msg)
                return

        # === Adverse selection defense (vol-adaptive) ===
        effective_adverse = self.adverse_threshold * (self._vol_ratio ** self.adverse_adapt_power)
        xvenue_mom = self._calc_xvenue_momentum()
        if abs(xvenue_mom) > effective_adverse:
            # Cancel opposing quote, keep same-direction quote
            if xvenue_mom > 0:
                # market going up -> cancel ask (avoid being short-squeezed)
                if self.ask_order.is_live:
                    self.ask_order = await self.client.cancel_order(
                        self.ask_order.order_id, timestamp=ts,
                        src_type="Rate", src_timestamp=ts, src_id=msg.get("universal_id"))
            else:
                # market going down -> cancel bid
                if self.bid_order.is_live:
                    self.bid_order = await self.client.cancel_order(
                        self.bid_order.order_id, timestamp=ts,
                        src_type="Rate", src_timestamp=ts, src_id=msg.get("universal_id"))
            return

        # === Calculate fair value and spread ===
        fair_value = self._calc_fair_value()
        dynamic_spread = self._calc_dynamic_spread()
        direction_bias = self._calc_direction_bias()

        # Position skew
        pos_skew = 0.0
        if self.max_position > 0:
            pos_skew = -cur_pos / self.max_position * self.skew_coef

        # Quote prices
        new_bid = self.my_floor(fair_value * (1 - (dynamic_spread - direction_bias - pos_skew) / 1e4))
        new_ask = self.my_ceil(fair_value * (1 + (dynamic_spread + direction_bias + pos_skew) / 1e4))

        # Ensure bid <= market best_bid and ask >= market best_ask
        best_bid = msg.get("best_bid", self.mid)
        best_ask = msg.get("best_ask", self.mid)
        new_bid = min(new_bid, best_bid)
        new_ask = max(new_ask, best_ask)

        # === Position-based order management ===
        if cur_pos == 0:
            # No position: quote both sides
            can_buy = True
            can_sell = True
        elif abs(cur_pos) >= self.max_position:
            # Max position: only exit
            can_buy = cur_pos < 0  # can buy to close short
            can_sell = cur_pos > 0  # can sell to close long
        else:
            can_buy = True
            can_sell = True

        # === Update bid quote ===
        if can_buy:
            bid_changed = (
                not self.bid_order.is_live or
                abs(new_bid - self.bid_order.price) > 10 ** self.decimal or
                (ts - self.bid_order.timestamp) > datetime.timedelta(seconds=2)
            )
            if bid_changed:
                if self.bid_order.is_live:
                    self.bid_order = await self.client.cancel_order(
                        self.bid_order.order_id, timestamp=ts,
                        src_type="Rate", src_timestamp=ts, src_id=msg.get("universal_id"))
                if not self.bid_order.is_live and not self.client.pending_positions:
                    self.bid_order = await self.client.send_order(
                        ts, self.sym, side=1, price=new_bid, amount=self.amount,
                        order_type=OrderType.Limit, src_type="Rate",
                        src_timestamp=ts, src_id=msg.get("universal_id"), misc="bid")
        elif self.bid_order.is_live:
            self.bid_order = await self.client.cancel_order(
                self.bid_order.order_id, timestamp=ts,
                src_type="Rate", src_timestamp=ts, src_id=msg.get("universal_id"))

        # === Update ask quote ===
        if can_sell:
            ask_changed = (
                not self.ask_order.is_live or
                abs(new_ask - self.ask_order.price) > 10 ** self.decimal or
                (ts - self.ask_order.timestamp) > datetime.timedelta(seconds=2)
            )
            if ask_changed:
                if self.ask_order.is_live:
                    self.ask_order = await self.client.cancel_order(
                        self.ask_order.order_id, timestamp=ts,
                        src_type="Rate", src_timestamp=ts, src_id=msg.get("universal_id"))
                if not self.ask_order.is_live and not self.client.pending_positions:
                    self.ask_order = await self.client.send_order(
                        ts, self.sym, side=-1, price=new_ask, amount=self.amount,
                        order_type=OrderType.Limit, src_type="Rate",
                        src_timestamp=ts, src_id=msg.get("universal_id"), misc="ask")
        elif self.ask_order.is_live:
            self.ask_order = await self.client.cancel_order(
                self.ask_order.order_id, timestamp=ts,
                src_type="Rate", src_timestamp=ts, src_id=msg.get("universal_id"))

    async def _force_close(self, msg: Dict):
        """Force close position via market order."""
        ts = msg.get("timestamp")
        cur_pos = self.cur_pos
        uid = msg.get("universal_id")

        # Cancel all live orders
        await self._cancel_all_quotes(msg)

        # Send market order to close
        if cur_pos > 0:
            self.exit_order = await self.client.send_order(
                ts, self.sym, side=-1, price=None, amount=abs(cur_pos),
                order_type=OrderType.Market, src_type="Rate",
                src_timestamp=ts, src_id=uid, misc="force_close")
        elif cur_pos < 0:
            self.exit_order = await self.client.send_order(
                ts, self.sym, side=1, price=None, amount=abs(cur_pos),
                order_type=OrderType.Market, src_type="Rate",
                src_timestamp=ts, src_id=uid, misc="force_close")
        self._entry_time = None

    async def _cancel_all_quotes(self, msg: Dict):
        """Cancel all live bid/ask orders."""
        ts = msg.get("timestamp")
        uid = msg.get("universal_id")
        if self.bid_order.is_live:
            self.bid_order = await self.client.cancel_order(
                self.bid_order.order_id, timestamp=ts,
                src_type="Rate", src_timestamp=ts, src_id=uid)
        if self.ask_order.is_live:
            self.ask_order = await self.client.cancel_order(
                self.ask_order.order_id, timestamp=ts,
                src_type="Rate", src_timestamp=ts, src_id=uid)
