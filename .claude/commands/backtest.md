# /backtest スキル

このスキルはバックテストの実行手順と注意点を案内します。参考実装: `notebooks/DevMM-backtest.ipynb`

## ステップ 1: インポート

```python
from etr.auto_import import *
from etr.core.fifo import calc_matching_table
from etr.data.data_loader import load_data
import pytz, gc

HTML(full_width_display)
```

## ステップ 2: データ準備

```python
ST    = datetime.datetime(2025, 6, 29, 0, tzinfo=pytz.utc)
ET    = datetime.datetime(2025, 6, 29, 23, tzinfo=pytz.utc)
VENUE = "bitbank"
SYM   = "BTCJPY"

rate   = load_data(date=[ST, ET], table="Rate",        venue=VENUE, symbol=SYM)
trades = load_data(date=[ST, ET], table="MarketTrade", venue=VENUE, symbol=SYM)
book   = load_data(date=[ST, ET], table="MarketBook",  venue=VENUE, symbol=SYM)
```

## ステップ 3: BT メッセージ作成

```python
# BT_Rate: 1秒リサンプル (推奨: メモリ制約のため)
bt_rate_1s = (
    rate.set_index("market_created_timestamp", drop=False)
    .groupby(["sym", "venue"])[["best_bid", "best_ask"]]
    .resample("1s", label="right").last()
    .unstack(level=[0, 1]).ffill()
    .stack(level=[1, 2]).reset_index()
)
# market_created_timestamp を index として用いるが、NULLの場合には必要に応じてtimestampで代用する.
bt_rate  = [(r.market_created_timestamp, {**r.to_dict(), "_data_type": "BT_Rate"})
            for _, r in bt_rate_1s.iterrows()]
bt_trade = [(r.market_created_timestamp, {**r.to_dict(), "_data_type": "BT_MarketTrade"})
            for _, r in trades.iterrows()]
bt_book  = [(r.market_created_timestamp, {**r.to_dict(), "_data_type": "BT_MarketBook"})
            for _, r in book.iterrows()]

messages = sorted(bt_rate + bt_trade + bt_book, key=lambda x: x[0])
```

## ステップ 4: バックテスト実行

```python
from etr.core.api_client.backtest_client import BacktestClient
from etr.strategy.dev_mm.dev_mm_v1 import DevMMv1  # 使用する戦略に変える

client   = BacktestClient(VENUE)
strategy = DevMMv1(sym=SYM, venue=VENUE, client=client, ...)
client.register_strategy(strategy)
strategy.warmup(now=ST)

# メッセージループ: client → strategy の順序を厳守
for t, msg in tqdm(messages):
    await strategy.client.on_message(msg)
    await strategy.on_message(msg)
```

## ステップ 5: PnL 計算

```python
mat = calc_matching_table(client.transactions_frame)
# 主要カラム: pl_bp, horizon, rinc_price, rdec_price,
#             rinc_order_type, rdec_order_type, amount, side

# 手数料を加算してネット PnL を計算 (例: bitbank)
FEE = {"limit": -2, "market": 12}  # bps
mat["fee_bp"] = (mat.rinc_order_type.map(FEE) + mat.rdec_order_type.map(FEE))
mat["pl_net"]     = mat.pl_bp + mat.fee_bp
mat["pl_net_jpy"] = mat.pl_net * mat.amount * mat.rinc_price / 1e4

print(f"Total PnL: {mat.pl_net_jpy.sum():.2f} JPY")
print(f"Win Rate : {(mat.pl_net > 0).mean() * 100:.1f}%")
```

---

## 重要な注意点

### BacktestClient
- `client.register_strategy(strategy)` を忘れずに実行

### タイムスタンプ
- シグナル生成や戦略の意思決定は必ず `timestamp` (受信時刻) に基づいて行う
- `market_created_timestamp` は将来情報の先読みになりうるため BT では使わない

### メモリ管理 (64GB 制限)
```python
for week_start in week_list:
    week_end = week_start + datetime.timedelta(days=7)
    rate   = load_data(date=[week_start, week_end], ...)
    trades = load_data(date=[week_start, week_end], ...)
    # ... バックテスト実行 ...
    del rate, trades, book, messages
    gc.collect()
```

### BitFlyer 板寄せ
19:05–19:15 UTC の Rate/MarketBook は除外推奨:
```python
rate = rate[~rate.timestamp.dt.time.between(
    datetime.time(19, 5), datetime.time(19, 15)
)]
```
