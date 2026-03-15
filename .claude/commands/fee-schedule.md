# /fee-schedule スキル

このスキルは取引所別の手数料体系を案内します。

## 手数料一覧 (bps = 0.01%)

| venue | 種別 | Maker | Taker | 備考 |
|-------|------|------:|------:|------|
| GMO | レバレッジ (XXXJPY) | 0 bps | 0 bps | BTCJPY, ETHJPY 等 |
| GMO | 現物 (BTC,ETH,XRP,DAI) | **-1 bps** | 5 bps | BTC, ETH 等 (JPYなし) |
| GMO | 現物 (その他) | **-3 bps** | 9 bps | 上記以外のJPYなしsymbol |
| BitFlyer | FXBTCJPY | 0 bps | 0 bps | |
| BitFlyer | その他 | 15 bps | 15 bps | |
| BitBank | BTCJPY | 0 bps | 10 bps | |
| BitBank | その他 | **-2 bps** | 12 bps | |
| HyperLiquid | 全般 | 1.5 bps | 4.5 bps | |

> **Maker = 指値注文 (板に乗せる)、Taker = 成行注文 (板を取る)**
> 負の値はリベート (手数料を受け取れる)

---

## バックテストでの手数料計算

```python
mat = calc_matching_table(client.transactions_frame)

# rinc_order_type / rdec_order_type: "limit" or "market"
FEE_MAP = {"limit": -2, "market": 12}  # bitbank の例 (bps)

mat["fee_bp"]     = mat.rinc_order_type.map(FEE_MAP) + mat.rdec_order_type.map(FEE_MAP)
mat["pl_net"]     = mat.pl_bp + mat.fee_bp
mat["pl_net_jpy"] = mat.pl_net * mat.amount * mat.rinc_price / 1e4
```

### 取引所別の FEE_MAP
```python
# GMO レバレッジ (BTCJPY 等)
FEE_MAP_GMO_LEV = {"limit": 0, "market": 0}

# GMO 現物 (BTC 等)
FEE_MAP_GMO_SPOT = {"limit": -1, "market": 5}

# BitBank
FEE_MAP_BITBANK = {"limit": -2, "market": 12}

# HyperLiquid
FEE_MAP_HL = {"limit": 1.5, "market": 4.5}
```

---

## 手数料が戦略に与える影響

- **BitBank**: Maker リベートが大きい (-2 bps) → 指値戦略が有利
- **GMO レバレッジ**: 手数料ゼロ → 価格差が収益の全て
- **HyperLiquid**: Maker も手数料発生 (1.5 bps) → スプレッドが小さいと厳しい
