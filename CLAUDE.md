# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

etrading is a Python-based algorithmic trading framework for cryptocurrency and FX markets. It handles real-time market data from multiple exchanges, executes trading strategies, and stores historical data.

## Common Commands

```bash
# Install package in editable mode
pip install -e .

# Install dependencies
pip install -r requirements.txt

# Run feed process (market data aggregator)
python scripts/feed_process.py

# Run a trading strategy
python scripts/run_imb_mm_bb_btc.py
python scripts/run_dev_mm_v1.py

# Run HDB (Historical Database) dump loop
python scripts/hdb_loop.py
```

## Architecture

### Data Flow

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

### Key Data Types (`src/etr/core/datamodel/datamodel.py`)

- `MarketTrade`: Market execution events
- `Rate`: Best bid/ask snapshot
- `MarketBook`: Full order book
- `Order`: Order state tracking (Sent → New → Partial → Filled/Canceled)
- `Trade`: Own execution records

### Strategy Implementation

Strategies extend `StrategyBase` (`src/etr/strategy/base_strategy.py`) and implement:
```python
@abstractmethod
def on_message(self, msg: Dict):
    # Handle incoming messages by _data_type:
    # "Rate", "MarketBook", "MarketTrade", "Order", "Trade"
```

### Subscription Model

LocalWsClient subscribes by filter dictionaries with keys: `_data_type`, `venue`, `sym`
```python
await subscriber.subscribe([
    {"_data_type": "Rate", "venue": "bitbank", "sym": "BTCJPY"},
    {"_data_type": "MarketBook", "venue": "bitbank", "sym": "BTCJPY"},
])
```

## Configuration

Environment variables in `.env`:
- `LOG_DIR`, `TP_DIR`, `HDB_DIR`: Data directories
- `{EXCHANGE}_API_KEY`, `{EXCHANGE}_API_SECRET`: Exchange credentials
- `DISCORD_URL_*`: Notification webhooks
- `JQUANTS_*`: J-Quants API for Japanese equities

全てのenv変数はPython runtimeの場合、`etr.config.Config` から利用できる.

## Instructions
* 現状の作業の状態は適宜`CLAUDE_LOG.md`に書き込みしてね.
* ipynbを作成して分析を実行したときは、
    * Add this command for all analysis notebooks. And make sure to insert Japanese summary comment (what you do) in just after each ipynb notebook chapter.
    ```python
    from etr.auto_import import *
    HTML(full_width_display)
    ```
    * Make sure your new notebook has output cell (to see the result immediately)
    * Set this option `--ExecutePreprocessor.timeout=3600` to avoid timeout 
* NeuralNetworkのフレームワークはtorchをつかってね.
* JQuantsの `markets_margin_interest` tableは週次のデータが翌週の第二営業日の夜間に更新される一方、`Date`列は金曜日を示しているので注意.

## Backtest Tips
### Exchange Transaction Fees (*not including Transaction Cost)
- BitBank
    - Maker: -2bps
    - Taker: 12bps
- BitFlyer:
    - FXBTCJPY
        - Maker: 0bps
        - Taker: 0bps
    - Others:
        - Maker: 2bps (?)
        - Taker: 10bps (?)
- HyperLiquid
    - Taker: 4.5bps
    - Maker: 1.5bps
- GMO
    - XXXJPY (leverage ccypairs e.g. BTCJPY, ETHJPY)
        - Taker: 0bps
        - Maker: 0bps
    - XXX (spot ccypairs e.g. BTC, ETH)
        - Maker: -1bps
        - Taker: 5bps

### Backtest Sample
実装したStrategyをrigorousにBacktestするNotebook: `notebooks/DevMM-backtest.ipynb`
走らせるにはMarketBookやMarketTradeのデータがフルで必要なのでメモリリークに注意.
1week程度ずつ走らせるのが推奨(日曜~土曜の7日間ずつ).
また、venue=bitflyerについては19:10UTC前後に板寄せがあるため`Rate`, `MarketBook`のデータに大きなジャンプが入るのでクレンジング推奨.


## Data Storage

- **TP logs**: `data/tp/TP-{HandlerClass}-{Symbol}.log` (JSON lines with `timestamp||{json}` format)
- **HDB**: `data/hdb/{table}/{venue}/{YYYY-MM-DD}/{symbol}/*.parquet`

### Supported Exchanges

BitBank, BitMex, BitFlyer, Binance, Coincheck, GMO (FX/Crypto), Hyperliquid

### Table List (Crypt)
- CryptのHDBは`etr.data.data_loader.load_data(date, table, venue, symbol)`でparquetをロードできる
- CryptのHDBは`etr.data.data_loader.list_table(date)`でその日のデータリストを確認できる
- JQuantsのDataは `etr.data.jquants.sqlite.get(query)`でロードでき、 DDLはここから確認できる`src/etr/data/jquants/ddl/jquants_v2_ddl_00.sql`

2026/01/10現在のCryptのテーブル一覧は下記 ((*Hyperliquid除く.これは大量の通貨ペアでCandlesテーブルが利用可能.):
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


