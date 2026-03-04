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
jupyter nbconvert --to notebook --execute --ExecutePreprocessor.timeout=3600 notebook.ipynb
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

### データストレージ

- **HDB**: `data/hdb/{table}/{venue}/{YYYY-MM-DD}/{symbol}/*.parquet`
- **TP logs**: `data/tp/TP-{HandlerClass}-{Symbol}.log` (`timestamp||{json}` format)
- **読込**: `load_data(date=[start, end], table, venue, symbol)` → DataFrame
- **一覧**: `list_table(date)` でその日の利用可能テーブル確認
- **JQuants**: `etr.data.jquants.sqlite.get(query)`, DDLは `src/etr/data/jquants/ddl/jquants_v2_ddl_00.sql`

### タイムスタンプ規則

- `timestamp`: 受信時刻 (トレード環境基準、バックテストではこちらを使う)
- `market_created_timestamp`: 取引所タイムスタンプ
- 将来情報の先読み厳禁: シグナル生成には必ず `timestamp` を基準にする

### 設定 (`config.py`)

`.env` から認証情報を読み込み。`Config.BITFLYER_API_KEY`, `Config.GMO_API_KEY`, `Config.TP_DIR`, `Config.HDB_DIR` 等。

## 重要なパターンと注意点

### BacktestClient の初期化

`BacktestClient` は `super().__init__()` を呼ばないため、手動で設定が必要:
```python
client = BacktestClient(venue="gmo")
client.pending_positions = False  # 必須
```

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
```

## 取引手数料

| Venue | 種別 | Maker | Taker |
|-------|------|-------|-------|
| GMO | レバレッジ (XXXJPY) | 0bps | 0bps |
| GMO | 現物 (XXX) | -1bps | 5bps |
| BitFlyer | FXBTCJPY | 0bps | 0bps |
| BitBank | 全般 | -2bps | 12bps |
| HyperLiquid | 全般 | 1.5bps | 4.5bps |

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
- フィード処理: `scripts/feed_process.py`
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
2026/01/10現在のCryptのテーブル一覧は下記 ((*Hyperliquid除く.これは大量の通貨ペアでCandlesテーブルが利用可能).
| table           | venue       | sym        |
|:----------------|:------------|:-----------|
| MarketTrade     | bitmex      | LTCUSD     |
| MarketTrade     | bitmex      | XBTUSD     |
| MarketTrade     | bitmex      | DOGEUSD    |
| MarketTrade     | bitmex      | ETHUSD     |
| MarketTrade     | bitmex      | XRPUSD     |
| MarketTrade     | bitmex      | SOLUSD     |
| MarketTrade     | binance     | ETHUSDT    |
| MarketTrade     | binance     | DOGEUSDT   |
| MarketTrade     | binance     | SOLUSDT    |
| MarketTrade     | binance     | BTCUSDT    |
| MarketTrade     | binance     | XRPUSDT    |
| MarketTrade     | binance     | LTCUSDT    |
| MarketTrade     | bitflyer    | BTCJPY     |
| MarketTrade     | bitflyer    | FXBTCJPY   |
| MarketTrade     | bitflyer    | XRPJPY     |
| MarketTrade     | bitflyer    | ETHBTC     |
| MarketTrade     | bitflyer    | ETHJPY     |
| MarketTrade     | gmo         | LTC        |
| MarketTrade     | gmo         | BTC        |
| MarketTrade     | gmo         | DOGEJPY    |
| MarketTrade     | gmo         | BTCJPY     |
| MarketTrade     | gmo         | DOGE       |
| MarketTrade     | gmo         | SOL        |
| MarketTrade     | gmo         | XRPJPY     |
| MarketTrade     | gmo         | SOLJPY     |
| MarketTrade     | gmo         | ETH        |
| MarketTrade     | gmo         | LTCJPY     |
| MarketTrade     | gmo         | XRP        |
| MarketTrade     | gmo         | ETHJPY     |
| MarketTrade     | hyperliquid | HYPE       |
| MarketTrade     | hyperliquid | BTC        |
| MarketTrade     | hyperliquid | ETH        |
| MarketTrade     | hyperliquid | HYPEUSDC   |
| MarketTrade     | bitbank     | DOGEJPY    |
| MarketTrade     | bitbank     | BTCJPY     |
| MarketTrade     | bitbank     | XRPJPY     |
| MarketTrade     | bitbank     | SOLJPY     |
| MarketTrade     | bitbank     | LTCJPY     |
| MarketTrade     | bitbank     | ETHJPY     |
| MarketLiquidity | bitmex      | LTCUSD     |
| MarketLiquidity | bitmex      | XBTUSD     |
| MarketLiquidity | bitmex      | DOGEUSD    |
| MarketLiquidity | bitmex      | ETHUSD     |
| MarketLiquidity | bitmex      | XRPUSD     |
| MarketLiquidity | bitmex      | SOLUSD     |
| MarketLiquidity | bitflyer    | BTCJPY     |
| MarketLiquidity | bitflyer    | FXBTCJPY   |
| MarketLiquidity | bitflyer    | XRPJPY     |
| MarketLiquidity | bitflyer    | ETHJPY     |
| MarketLiquidity | gmo         | DOGEJPY    |
| MarketLiquidity | gmo         | BTCJPY     |
| MarketLiquidity | gmo         | XRPJPY     |
| MarketLiquidity | gmo         | SOLJPY     |
| MarketLiquidity | gmo         | LTCJPY     |
| MarketLiquidity | gmo         | ETHJPY     |
| MarketLiquidity | bitbank     | DOGEJPY    |
| MarketLiquidity | bitbank     | BTCJPY     |
| MarketLiquidity | bitbank     | XRPJPY     |
| MarketLiquidity | bitbank     | SOLJPY     |
| MarketLiquidity | bitbank     | LTCJPY     |
| MarketLiquidity | bitbank     | ETHJPY     |
| CircuitBreaker  | bitbank     | DOGEJPY    |
| CircuitBreaker  | bitbank     | BTCJPY     |
| CircuitBreaker  | bitbank     | XRPJPY     |
| CircuitBreaker  | bitbank     | SOLJPY     |
| CircuitBreaker  | bitbank     | LTCJPY     |
| CircuitBreaker  | bitbank     | ETHJPY     |
| MarketBook      | bitmex      | LTCUSD     |
| MarketBook      | bitmex      | XBTUSD     |
| MarketBook      | bitmex      | DOGEUSD    |
| MarketBook      | bitmex      | ETHUSD     |
| MarketBook      | bitmex      | XRPUSD     |
| MarketBook      | bitmex      | SOLUSD     |
| MarketBook      | bitflyer    | BTCJPY     |
| MarketBook      | bitflyer    | FXBTCJPY   |
| MarketBook      | bitflyer    | XRPJPY     |
| MarketBook      | bitflyer    | ETHBTC     |
| MarketBook      | bitflyer    | ETHJPY     |
| MarketBook      | gmo         | LTC        |
| MarketBook      | gmo         | BTC        |
| MarketBook      | gmo         | DOGEJPY    |
| MarketBook      | gmo         | BTCJPY     |
| MarketBook      | gmo         | DOGE       |
| MarketBook      | gmo         | SOL        |
| MarketBook      | gmo         | XRPJPY     |
| MarketBook      | gmo         | SOLJPY     |
| MarketBook      | gmo         | ETH        |
| MarketBook      | gmo         | LTCJPY     |
| MarketBook      | gmo         | XRP        |
| MarketBook      | gmo         | ETHJPY     |
| MarketBook      | bitbank     | DOGEJPY    |
| MarketBook      | bitbank     | BTCJPY     |
| MarketBook      | bitbank     | XRPJPY     |
| MarketBook      | bitbank     | SOLJPY     |
| MarketBook      | bitbank     | LTCJPY     |
| MarketBook      | bitbank     | ETHJPY     |
| Rate            | bitmex      | LTCUSD     |
| Rate            | bitmex      | XBTUSD     |
| Rate            | bitmex      | DOGEUSD    |
| Rate            | bitmex      | ETHUSD     |
| Rate            | bitmex      | XRPUSD     |
| Rate            | bitmex      | SOLUSD     |
| Rate            | binance     | ETHUSDT    |
| Rate            | binance     | DOGEUSDT   |
| Rate            | binance     | SOLUSDT    |
| Rate            | binance     | BTCUSDT    |
| Rate            | binance     | XRPUSDT    |
| Rate            | binance     | LTCUSDT    |
| Rate            | bitflyer    | BTCJPY     |
| Rate            | bitflyer    | FXBTCJPY   |
| Rate            | bitflyer    | XRPJPY     |
| Rate            | bitflyer    | ETHBTC     |
| Rate            | bitflyer    | ETHJPY     |
| Rate            | gmo         | AUDUSD     |
| Rate            | gmo         | LTC        |
| Rate            | gmo         | BTC        |
| Rate            | gmo         | DOGEJPY    |
| Rate            | gmo         | BTCJPY     |
| Rate            | gmo         | CHFJPY     |
| Rate            | gmo         | DOGE       |
| Rate            | gmo         | CADJPY     |
| Rate            | gmo         | AUDJPY     |
| Rate            | gmo         | GBPUSD     |
| Rate            | gmo         | EURJPY     |
| Rate            | gmo         | SOL        |
| Rate            | gmo         | XRPJPY     |
| Rate            | gmo         | SOLJPY     |
| Rate            | gmo         | GBPJPY     |
| Rate            | gmo         | ETH        |
| Rate            | gmo         | LTCJPY     |
| Rate            | gmo         | XRP        |
| Rate            | gmo         | ETHJPY     |
| Rate            | gmo         | EURUSD     |
| Rate            | bitbank     | DOGEJPY    |
| Rate            | bitbank     | BTCJPY     |
| Rate            | bitbank     | XRPJPY     |
| Rate            | bitbank     | SOLJPY     |
| Rate            | bitbank     | LTCJPY     |
| Rate            | bitbank     | ETHJPY     |
