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

### Data Storage

- **TP logs**: `data/tp/TP-{HandlerClass}-{Symbol}.log` (JSON lines with `timestamp||{json}` format)
- **HDB**: `data/hdb/{table}/{venue}/{YYYY-MM-DD}/{symbol}/*.parquet`
- Load data via `etr.data.data_loader.load_data(date, table, venue, symbol)`
- Load JQuants Database via `etr.data.jquants.sqlite.get(query)`. You can see DDL under `etr.data.jquants.ddl`.

### Supported Exchanges

BitBank, BitMex, BitFlyer, Binance, Coincheck, GMO (FX/Crypto), Hyperliquid

## Configuration

Environment variables in `.env`:
- `LOG_DIR`, `TP_DIR`, `HDB_DIR`: Data directories
- `{EXCHANGE}_API_KEY`, `{EXCHANGE}_API_SECRET`: Exchange credentials
- `DISCORD_URL_*`: Notification webhooks
- `JQUANTS_*`: J-Quants API for Japanese equities

## Instructions
* 現状の作業の状態は適宜`CLAUDE_LOG.md`に書き込みしてね. あと、ipynbを実行した時にはhtmlに結果をexportしといてね.
* ipynbを作成して分析を実行したときは、
    * Add this command for all analysis notebooks. And make sure to insert Japanese summary comment (what you do) in just after each ipynb notebook chapter.
    ```python
    from etr.auto_import import *
    HTML(full_width_display)
    ```
    * Make sure your new notebook has output cell (to see the result immediately)
* NeuralNetworkのフレームワークはtorchをつかってね.

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

### Backtest Sample
実装したStrategyをrigorousにBacktestするNotebook: `notebooks/DevMM-backtest.ipynb`
走らせるにはMarketBookやMarketTradeのデータがフルで必要なのでメモリリークに注意.
1week程度ずつ走らせるのが推奨. ただしCryptについては平日土日かかわらず24H/365Dでデータがあるので注意.
また、venue=bitflyerについては19:10UTC前後に板寄せがあるため`Rate`, `MarketBook`のデータに大きなジャンプが入るのでクレンジング推奨.
