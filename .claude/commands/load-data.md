# /load-data スキル

このスキルは `etrading` プロジェクトでのデータ読み込み方法を案内します。

## Crypt HDB データの読み込み

```python
from etr.data.data_loader import load_data

# 単一日付
df = load_data(date="2025-06-29", table="Rate", venue="gmo", symbol="BTCJPY")

# 日付範囲
df = load_data(
    date=["2025-06-01", "2025-06-30"],
    table="MarketTrade",
    venue="bitbank",
    symbol="BTCJPY",
)

# 全取引所 (venue ワイルドカード)
df = load_data(date="2025-06-29", table="Rate", venue="*", symbol="BTCJPY")
```

### 主なテーブル
| table | 内容 |
|-------|------|
| `Rate` | best_bid, best_ask, mid_price, spread |
| `MarketTrade` | side, price, amount, trade_id |
| `MarketBook` | bids/asks の板情報 (numpy配列) |
| `MarketLiquidity` | 板の流動性集計 |
| `CircuitBreaker` | サーキットブレーカー発動履歴 |

### タイムスタンプ
- `timestamp`: **受信時刻** → バックテストのシグナル生成に使う (将来情報漏れ防止)
- `market_created_timestamp`: 取引所タイムスタンプ

### MarketBook の扱い
```python
# bids/asks は numpy配列 [[price, size], ...]
bids = row["bids"]
if len(bids) > 0:            # ← if bids は NG
    best_bid_price = float(bids[0][0])
    best_bid_size  = float(bids[0][1])
```

---

## JQuants データの読み込み

JQuants データは **SQLite DB** に蓄積されており、`sqlite.get(query)` で SQL を直接実行するのが基本。
API 経由の関数 (`api_v2`) は主に DB への取り込みスクリプトで使用する。

### SQLite から読み込む (推奨)

```python
from etr.data.jquants.sqlite import get

# 日足 OHLCV (権利修正済み)
df = get("SELECT * FROM equities_bars_daily WHERE Code = '72030' ORDER BY Date")

# 銘柄マスター + 日足を結合 (DailyOHLC VIEW を使うと便利)
df = get("""
    SELECT * FROM DailyOHLC
    WHERE Source = 'spot'
      AND Date BETWEEN '2024-01-01' AND '2024-12-31'
      AND Code = '72030'
    ORDER BY Date
""")

# 財務サマリー (決算発表ごと)
df = get("""
    SELECT DiscDate, Code, DocType, CurFYEn, Sales, OP, NP, EPS, BPS
    FROM fins_summary
    WHERE Code = '72030'
    ORDER BY DiscDate
""")

# 信用残高 (週次)
df = get("""
    SELECT * FROM markets_margin_interest
    WHERE Code = '72030'
    ORDER BY Date
""")

# 取引カレンダー
df = get("SELECT * FROM markets_calendar WHERE HolDiv = '1'")  -- 1=営業日
```

### SQLite テーブル一覧

| テーブル / VIEW | 内容 | 主キー |
|----------------|------|--------|
| `equities_master` | 銘柄マスター (社名, 市場区分, 業種コード S17/S33) | Date, Code |
| `equities_bars_daily` | 日足 OHLCV + 権利修正済み (AdjO/H/L/C/Vo) | Date, Code |
| `equities_bars_minute` | 分足 OHLCV | Date, Code, Time |
| `equities_earnings_calendar` | 決算予定日 | Date, Code, FY, FQ |
| `fins_summary` | 決算財務数値 (Sales/OP/NP/EPS/BPS/配当など) | DiscDate, Code, DiscNo |
| `indices_bars_daily` | 指数日足 (日経平均, TOPIX 等) | Date, Code |
| `indices_info` | 指数マスター | Code, IndexName |
| `markets_calendar` | 取引日カレンダー (HolDiv=1: 営業日) | Date, HolDiv |
| `index_option_N225` | 日経225オプション日足 (IV, OI, Strike 等) | Date, Code, EmMrgnTrgDiv |
| `markets_margin_interest` | 信用残高 (買/売, 制度/一般) 週次 | Date, Code |
| `markets_short_ratio` | 空売り比率 (業種別) 日次 | Date, S33 |
| `markets_short_sale_report` | 大量空売り報告 | DiscDate, Code, SSName, CalcDate |
| `markets_margin_alert` | 信用取引注意銘柄 | PubDate, Code, AppDate |
| **`DailyOHLC`** (VIEW) | spot + index + index_opt を統合した便利ビュー | - |

### v2 API (DB 取り込み用)

```python
from etr.data.jquants.api_v2 import (
    get_equities_master,
    get_equities_bars_daily,
    get_equities_earnings_calendar,
)

# 日足 OHLCV (APIから直接取得したい場合)
df_daily = get_equities_bars_daily(code="72030", from_="2024-01-01", to_="2024-12-31")
```

### JQuants の注意点
- `markets_margin_interest` は**週次データが翌週第二営業日夜間**に更新
- `Date` 列は**金曜日**を示しているため、更新タイミングとのズレに注意
- コードは 5 桁 (`"72030"`) で統一 (4 桁 `"7203"` は API によって異なる場合あり)
- 認証情報は `.env` に `JQUANTS_API_KEY` として設定

---

## データの注意点・クレンジング

- **BitFlyer 板寄せ**: 19:05–19:15 UTC に Rate/MarketBook が跳ねる → この時間帯を除外推奨
- **メモリ管理**: バックテスト時は 1 週間ずつ読み込み、各ループ後に `gc.collect()` を呼ぶ (64GB 制限)
