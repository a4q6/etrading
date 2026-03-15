# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## プロジェクト概要

暗号資産・FXの高頻度取引(HFT)フレームワーク。マルチ取引所対応のマーケットメイキング戦略を開発・バックテスト・本番運用するためのシステム。

## 開発コマンド

```bash
# インストール (開発モード)
pip install -e .

# テスト実行
pytest tests/
pytest tests/test_gmo_private_api.py  # 単体テスト

# ノートブック実行 (タイムアウト延長必須)
jupyter nbconvert --to notebook --execute --ExecutePreprocessor.timeout=14400 notebook.ipynb
```

## アーキテクチャ

### コア構造 (`src/etr/`)

```
ExchangeClientBase (core/api_client/base.py)
  ├── BacktestClient (core/api_client/backtest_client.py) - バックテスト用
  ├── GmoRestClient (core/api_client/gmo/gmo.py)
  ├── BitFlyerClient (core/api_client/bitflyer/bitflyer.py)
  ├── BitbankClient (core/api_client/bitbank/bitbank.py)
  └── CoincheckClient (core/api_client/coincheck.py)

StrategyBase (strategy/base_strategy.py)
  └── VolatilityAdaptiveMM, CrossFee, ToD...

FeedHandler (core/feed_handler/) - Websocket接続
  ├── GMO (gmo_crypt.py, gmo_forex.py, gmo_private_stream.py)
  ├── BitFlyer (bitflyer.py, bitflyer_private_stream.py)
  ├── BitBank (bitbank_private_stream.py)
  ├── Binance (binance.py)
  ├── BitMex (bitmex.py)
  └── HyperLiquid (hyperliquid.py)
```

### メッセージフロー

```
[Exchange WebSockets] → [FeedHandler] → [LocalWsPublisher:8765] → [LocalWsClient] → [Strategy]
                                              ↓
                                      [TP Log Files] → [HdbDumper] → [HDB (Parquet)]
```

1. **Feed Handlers** (`src/etr/core/feed_handler/`) connect to exchange WebSockets
2. **LocalWsPublisher** aggregates all feeds and broadcasts via local WebSocket (port 8765)
3. **Strategies** subscribe to specific data types via **LocalWsClient**
4. **TP (Tick Print) logs** capture raw data in `data/tp/`
5. **HdbDumper** converts TP logs to Parquet files in `data/hdb/`

メッセージの `_data_type` フィールドで分岐: `Rate`, `MarketTrade`, `MarketBook`, `Order`

バックテスト時は `BT_Rate` (1s resample.last()) + `BT_MarketTrade` + `BT_MarketBook` がBacktestClientに送られる。

### データモデル (`core/datamodel/datamodel.py`)

- **Rate**: best_bid, best_ask, mid_price, spread, latency
- **MarketBook**: bids/asks (SortedDict[price, size])
- **MarketTrade**: side, price, amount, trade_id
- **Order / BacktestOrder**: order_type, order_status, executed_amount


### タイムスタンプ規則

- `timestamp`: 受信時刻 (トレード環境基準、バックテストではこちらを使う)
- `market_created_timestamp`: 取引所タイムスタンプ
- 将来情報の先読み厳禁: シグナル生成には必ず `timestamp` を基準にする

### 設定 (`config.py`)

`.env` から認証情報を読み込み。`Config.BITFLYER_API_KEY`, `Config.GMO_API_KEY`, `Config.TP_DIR`, `Config.HDB_DIR` 等。

## 重要なパターンと注意点

### MarketBook の扱い

bids/asks は numpy配列の `[price, size]` ペア。空チェックは `len(bids) > 0` を使う (`if bids` はNG)。値の取得は `float(bids[0][1])`。

### メモリ管理 (64GB制限)

バックテストは1週間(日曜〜土曜)ずつ実行し、各ループ後に `gc.collect()` を呼ぶ。

### BitFlyer板寄せ

19:05-19:15 UTC にRate/MarketBookが跳ねるため、この時間帯はクレンジング推奨。

### FIFO PnL計算

```python
from etr.core.fifo import calc_matching_table
df_matching = calc_matching_table(client.transactions_frame)
# カラム: pl_bp, horizon, rinc_price, rdec_price
# 詳細は /backtest スキルを参照
```

## 取引手数料

→ `/fee-schedule` スキルを参照

## ノートブック規約

- 先頭セルに必ず:
  ```python
  from etr.auto_import import *
  HTML(full_width_display)
  ```
- 各チャプター直後に日本語の要約コメントを挿入
- 出力セル(output cell)を必ず含める
- NNフレームワークはPyTorchを使用
- バックテストの参考実装: `notebooks/DevMM-backtest.ipynb`
- GMO HFT戦略の詳細要件: `notebooks/GMO-HFT/README.md`

## 本番運用

- 戦略スクリプト: `scripts/run_*.py`
- フィード処理: `scripts/feed_process.py` (rust版: `scripts/rust/rust_feed_process.sh`)
- Discord通知: `core/notification/discord.py`
- 非同期ログ: `AsyncBufferedLogger` (core/async_logger.py)
- CEP: EMA/OHLC/ImpactPriceのリアルタイム計算 (`core/cep/`)

## Tips

### Instructions
* 現状の作業の状態は適宜`CLAUDE_LOG.md`に書き込みしてね.
* ipynbを作成して分析を実行したときは、
    * Make sure your new notebook has output cell (to see the result immediately)
* NeuralNetworkのフレームワークはtorchをつかってね.
* JQuantsの `markets_margin_interest` tableは週次のデータが翌週の第二営業日の夜間に更新される一方、`Date`列は金曜日を示しているので注意

### データテーブルリスト

→ `/data-catalog` スキルを参照
