import numpy as np
from etr.core.api_client.backtest_client import BacktestClient
from etr.core.datamodel import Order, OrderType
from etr.strategy import StrategyBase
from collections import deque


class VolatilityAdaptiveMM(StrategyBase):
    """
    ボラティリティ適応型Market Making戦略

    改良点:
    1. ボラティリティに応じてスプレッド幅を動的調整
    2. 板寄せフィルター（JST 19:00-19:20 = UTC 10:00-10:20を除外）
    3. より保守的なベーススプレッド
    """
    def __init__(
        self,
        sym: str,
        venue: str,
        amount: float,
        client: BacktestClient,
        # スプレッドパラメータ
        base_offset: float = 3.0,  # ベースオフセット（bps）- 広めに設定
        vol_multiplier: float = 0.5,  # ボラティリティ乗数
        min_offset: float = 1.5,  # 最小オフセット
        max_offset: float = 10.0,  # 最大オフセット
        # ボラティリティ計算
        vol_window: int = 60,  # ボラティリティ計算ウィンドウ（tick数）
        # 板寄せフィルター（UTC時間、分単位）- JST 19:00-19:20を除外（余裕を持たせる）
        itayose_filter: tuple = (10, 0, 20),  # (hour, start_min, end_min)
        # ポジション管理
        max_position: float = 0.005,  # 最大ポジション（より厳格に）
        position_decay: float = 1.0,  # ポジション解消の強さ
        # リバーサルシグナル
        use_reversal: bool = True,
        reversal_lookback: int = 10,
        reversal_threshold: float = 5.0,
        reversal_factor: float = 0.3,
        model_id="VolAdaptiveMM",
    ):
        super().__init__(model_id, log_file=None)
        self.sym = sym
        self.venue = venue
        self.client = client
        self.amount = amount

        self.base_offset = base_offset
        self.vol_multiplier = vol_multiplier
        self.min_offset = min_offset
        self.max_offset = max_offset
        self.vol_window = vol_window
        self.itayose_filter = itayose_filter
        self.max_position = max_position
        self.position_decay = position_decay

        self.use_reversal = use_reversal
        self.reversal_lookback = reversal_lookback
        self.reversal_threshold = reversal_threshold
        self.reversal_factor = reversal_factor

        # 状態変数
        self.mid = np.nan
        self.price_hist = deque(maxlen=200)
        self.return_hist = deque(maxlen=200)
        self.bid_order = Order.null_order()
        self.ask_order = Order.null_order()
        self.current_vol = np.nan

    @property
    def cur_pos(self):
        return self.client.positions.get(self.sym, [0, 0])[1]

    def _is_itayose_time(self, ts) -> bool:
        """板寄せ時間帯（JST 19:00-19:20）かどうかをチェック"""
        hour, start_min, end_min = self.itayose_filter
        if ts.hour == hour and start_min <= ts.minute <= end_min:
            return True
        return False

    def _calc_volatility(self) -> float:
        """直近のボラティリティ（bps）を計算"""
        if len(self.return_hist) < 10:
            return 5.0  # デフォルト値
        returns = list(self.return_hist)[-self.vol_window:]
        return np.std(returns) if len(returns) > 1 else 5.0

    def _get_reversal_signal(self) -> float:
        """リバーサルシグナルを計算"""
        if not self.use_reversal or len(self.price_hist) < self.reversal_lookback + 1:
            return 0.0
        prices = list(self.price_hist)
        recent_return = (prices[-1] / prices[-self.reversal_lookback] - 1) * 1e4
        if abs(recent_return) > self.reversal_threshold:
            return -recent_return * self.reversal_factor / 10
        return 0.0

    def _calc_dynamic_offset(self) -> float:
        """ボラティリティに応じた動的オフセットを計算"""
        vol = self._calc_volatility()
        self.current_vol = vol
        # ボラティリティが高いほどスプレッドを広げる
        offset = self.base_offset + vol * self.vol_multiplier
        return np.clip(offset, self.min_offset, self.max_offset)

    async def on_message(self, msg):
        dtype = msg.get("_data_type")

        if dtype == "Order":
            if msg["order_id"] == self.bid_order.order_id:
                self.bid_order = Order(**{k: v for k, v in msg.items() if k != "_data_type"})
            if msg["order_id"] == self.ask_order.order_id:
                self.ask_order = Order(**{k: v for k, v in msg.items() if k != "_data_type"})

        elif dtype == "Rate" and msg["venue"] == self.venue and msg["sym"] == self.sym:
            # 板寄せ時間帯のデータは特徴量計算に使わない
            if not self._is_itayose_time(msg["timestamp"]):
                # 価格履歴更新
                if not np.isnan(self.mid):
                    ret = (msg["mid_price"] / self.mid - 1) * 1e4
                    self.return_hist.append(ret)
                self.mid = msg["mid_price"]
                self.price_hist.append(self.mid)

            await self._update_quotes(msg)

    async def _update_quotes(self, msg):
        if np.isnan(self.mid) or len(self.price_hist) < 20:
            return
        if len(self.client.pending_limit_orders) > 0:
            return

        # 板寄せ時間帯フィルター（JST 19:00-19:20 = UTC 10:00-10:20）
        if self._is_itayose_time(msg["timestamp"]):
            # 板寄せ時間帯：既存注文をキャンセル
            if self.bid_order.is_live:
                self.bid_order = await self.client.cancel_order(
                    self.bid_order.order_id, timestamp=msg["timestamp"],
                    src_type="Rate", src_timestamp=msg["timestamp"], src_id=msg.get("universal_id", ""))
            if self.ask_order.is_live:
                self.ask_order = await self.client.cancel_order(
                    self.ask_order.order_id, timestamp=msg["timestamp"],
                    src_type="Rate", src_timestamp=msg["timestamp"], src_id=msg.get("universal_id", ""))
            return

        # 動的オフセット計算
        base_offset = self._calc_dynamic_offset()

        # リバーサルシグナル
        reversal_signal = self._get_reversal_signal()

        # ポジションベースのスキュー
        pos_skew = -self.cur_pos / self.max_position * self.position_decay * 5

        total_skew = reversal_signal + pos_skew

        bid_offset = max(self.min_offset, base_offset - total_skew)
        ask_offset = max(self.min_offset, base_offset + total_skew)

        new_bid = np.floor(self.mid * (1 - bid_offset / 1e4))
        new_ask = np.ceil(self.mid * (1 + ask_offset / 1e4))

        can_buy = self.cur_pos < self.max_position
        can_sell = self.cur_pos > -self.max_position

        # Bid更新
        if can_buy:
            if not self.bid_order.is_live or abs(new_bid - self.bid_order.price) > 1:
                if self.bid_order.is_live:
                    self.bid_order = await self.client.cancel_order(
                        self.bid_order.order_id, timestamp=msg["timestamp"],
                        src_type="Rate", src_timestamp=msg["timestamp"], src_id=msg.get("universal_id", ""))
                if not self.bid_order.is_live:
                    self.bid_order = await self.client.send_order(
                        msg["timestamp"], self.sym, side=1, price=new_bid, amount=self.amount,
                        order_type=OrderType.Limit, src_type="Rate", src_timestamp=msg["timestamp"],
                        src_id=msg.get("universal_id", ""), misc="bid")
        elif self.bid_order.is_live:
            self.bid_order = await self.client.cancel_order(
                self.bid_order.order_id, timestamp=msg["timestamp"],
                src_type="Rate", src_timestamp=msg["timestamp"], src_id=msg.get("universal_id", ""))

        # Ask更新
        if can_sell:
            if not self.ask_order.is_live or abs(new_ask - self.ask_order.price) > 1:
                if self.ask_order.is_live:
                    self.ask_order = await self.client.cancel_order(
                        self.ask_order.order_id, timestamp=msg["timestamp"],
                        src_type="Rate", src_timestamp=msg["timestamp"], src_id=msg.get("universal_id", ""))
                if not self.ask_order.is_live:
                    self.ask_order = await self.client.send_order(
                        msg["timestamp"], self.sym, side=-1, price=new_ask, amount=self.amount,
                        order_type=OrderType.Limit, src_type="Rate", src_timestamp=msg["timestamp"],
                        src_id=msg.get("universal_id", ""), misc="ask")
        elif self.ask_order.is_live:
            self.ask_order = await self.client.cancel_order(
                self.ask_order.order_id, timestamp=msg["timestamp"],
                src_type="Rate", src_timestamp=msg["timestamp"], src_id=msg.get("universal_id", ""))
