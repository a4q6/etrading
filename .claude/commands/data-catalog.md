# /data-catalog スキル

このスキルは利用可能なデータテーブル一覧を案内します。(2026/01/10 現在)

## Crypt データ一覧

### Rate (ベストビッド/アスク)
mid_priceの変化ごとにサンプルされている.
| venue | symbol |
|-------|--------|
| gmo | BTCJPY, ETHJPY, XRPJPY, LTCJPY, SOLJPY, DOGEJPY, BTC, ETH, XRP, LTC, SOL, DOGE |
| gmo | AUDUSD, AUDJPY, CADJPY, CHFJPY, EURJPY, EURUSD, GBPJPY, GBPUSD (FX) |
| bitflyer | BTCJPY, FXBTCJPY, ETHJPY, XRPJPY, ETHBTC |
| bitbank | BTCJPY, ETHJPY, XRPJPY, LTCJPY, SOLJPY, DOGEJPY |
| binance | BTCUSDT, ETHUSDT, XRPUSDT, LTCUSDT, SOLUSDT, DOGEUSDT |
| bitmex | XBTUSD, ETHUSD, XRPUSD, LTCUSD, SOLUSD, DOGEUSD |

### MarketTrade (約定履歴)
| venue | symbol |
|-------|--------|
| gmo | BTCJPY, ETHJPY, XRPJPY, LTCJPY, SOLJPY, DOGEJPY, BTC, ETH, XRP, LTC, SOL, DOGE |
| bitflyer | BTCJPY, FXBTCJPY, ETHJPY, XRPJPY, ETHBTC |
| bitbank | BTCJPY, ETHJPY, XRPJPY, LTCJPY, SOLJPY, DOGEJPY |
| binance | BTCUSDT, ETHUSDT, XRPUSDT, LTCUSDT, SOLUSDT, DOGEUSDT |
| bitmex | XBTUSD, ETHUSD, XRPUSD, LTCUSD, SOLUSD, DOGEUSD |
| hyperliquid | BTC, ETH, HYPE, HYPEUSDC |

### MarketBook (板情報)
100ms あるいは 250ms のスロットリングごとにサンプルされている.
| venue | symbol |
|-------|--------|
| gmo | BTCJPY, ETHJPY, XRPJPY, LTCJPY, SOLJPY, DOGEJPY, BTC, ETH, XRP, LTC, SOL, DOGE |
| bitflyer | BTCJPY, FXBTCJPY, ETHJPY, XRPJPY, ETHBTC |
| bitbank | BTCJPY, ETHJPY, XRPJPY, LTCJPY, SOLJPY, DOGEJPY |
| bitmex | XBTUSD, ETHUSD, XRPUSD, LTCUSD, SOLUSD, DOGEUSD |

### MarketLiquidity (板流動性集計)
| venue | symbol |
|-------|--------|
| gmo | BTCJPY, ETHJPY, XRPJPY, LTCJPY, SOLJPY, DOGEJPY |
| bitflyer | BTCJPY, FXBTCJPY, ETHJPY, XRPJPY |
| bitbank | BTCJPY, ETHJPY, XRPJPY, LTCJPY, SOLJPY, DOGEJPY |
| bitmex | XBTUSD, ETHUSD, XRPUSD, LTCUSD, SOLUSD, DOGEUSD |

### CircuitBreaker (サーキットブレーカー)
| venue | symbol |
|-------|--------|
| bitbank | BTCJPY, ETHJPY, XRPJPY, LTCJPY, SOLJPY, DOGEJPY |

### その他
- **HyperLiquid**: Candles (1min OHLCV) テーブルが大量の通貨ペアで利用可能

---

## データ開始日 (目安)
| venue | table | 開始日 |
|-------|-------|--------|
| GMO | Rate | 2025-04-30 |
| GMO | MarketBook / MarketTrade | 2025-05-01 |
| BitFlyer | Rate (FXBTCJPY) | 2025-04-30 |
| Binance | Rate (BTCUSDT) | 2025-05-27 |
| BitMex | Rate (XBTUSD) | 2025-04-19 |

---

## GMO 現物シンボル vs レバレッジシンボルの区別
| 種別 | シンボル例 | 説明 |
|------|-----------|------|
| 現物 | `BTC`, `ETH`, `XRP` | JPY なし |
| レバレッジ | `BTCJPY`, `ETHJPY`, `XRPJPY` | JPY 付き |
