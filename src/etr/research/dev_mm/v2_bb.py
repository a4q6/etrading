import numpy as np
import pandas as pd
import numba
from numba.typed import Dict as NDict
from numba import types


@numba.njit
def my_ceil(x, n):
    factor = 10 ** n
    return np.ceil(x * factor) / factor


@numba.njit
def my_floor(x, n):
    factor = 10 ** n
    return np.floor(x * factor) / factor


@numba.jit(nopython=False)
def _run_dev_mm_bb(
    data: np.ndarray,
    # ['ask', 'bid', 'mid', 'buy', 'sell', 'best_bid', 'best_ask', 'dev_ma', 'dev_smooth', 'vol', 'ma_sign']
    idx: NDict,
    threshold=5,
    horizon=12 * 10,
    use_best_price_for_exit=True,
    exit_spread=3,
    sl_level=20,
    tp_level=20,
    vol_threshold=30,
    decimal=0,
):
    # Deviationが少ないときのみMM
    # run simulation
    pos = 0
    transactions = []
    exit_price = np.nan
    for i, row in enumerate(data):
        if pos != 0:
            # LIMIT EXIT
            if pos > 0:
                if np.isnan(exit_price):
                    exit_price = row[idx["best_ask"]] if use_best_price_for_exit else row[idx["mid"]]
                    exit_price = my_ceil(exit_price * (1 + exit_spread / 1e4), n=decimal)
                if exit_price <= row[idx["buy"]]:
                    transactions.append([i, row[idx["timestamp"]], -1, exit_price, 1, 1])  # [i, timestamp, side, price, trs_type, otype]
                    pos = 0
                    exit_price = np.nan
            elif pos < 0:
                if np.isnan(exit_price):
                    exit_price = row[idx["best_bid"]] if use_best_price_for_exit else row[idx["mid"]]
                    exit_price = my_floor(exit_price * (1 - exit_spread / 1e4), n=decimal)
                if exit_price >= row[idx["sell"]]:
                    transactions.append([i, row[idx["timestamp"]], +1, exit_price, 1, 1])  # [i, timestamp, side, price, trs_type, otype]
                    pos = 0
                    exit_price = np.nan
            # SL
            if pos != 0 and pos * (row[idx["mid"]] / transactions[-1][-3] - 1) * 1e4 < -sl_level:
                price = row[idx["best_bid"]] if pos > 0 else row[idx["best_ask"]]
                transactions.append([i, row[idx["timestamp"]], pos * -1, price, 2, 0])  # [i, timestamp, side, price, trs_type, otype]
                pos = 0
                exit_price = np.nan
            # TP
            price = row[idx["best_bid"]] if pos > 0 else row[idx["best_ask"]]
            if pos != 0 and pos * (price / transactions[-1][-3] - 1) * 1e4 > tp_level:
                transactions.append([i, row[idx["timestamp"]], pos * -1, price, 3, 0])  # [i, timestamp, side, price, trs_type, otype]
                pos = 0
                exit_price = np.nan
            # HORIZON
            if pos != 0 and transactions[-1][0] + horizon < i:
                price = row[idx["best_bid"]] if pos > 0 else row[idx["best_ask"]]
                transactions.append([i, row[idx["timestamp"]], pos * -1, price, 4, 0])  # [i, timestamp, side, price, trs_type, otype]
                pos = 0
                exit_price = np.nan
        else:
            if row[idx["vol"]] < vol_threshold and row[idx["dev_smooth"]] < threshold:
                # ask = my_ceil(np.maximum(row[idx["ask"]], row[idx["mid"]]), n=decimal)
                ask = row[idx["best_ask"]]
                if ask <= row[idx["buy"]]:
                    transactions.append([i, row[idx["timestamp"]], -1, ask, 0, 1])  # [i, timestamp, side, price, trs_type, otype]
                    pos -= 1

                # bid = my_floor(np.minimum(row[idx["bid"]], row[idx["mid"]]), n=decimal)
                bid = row[idx["best_bid"]]
                if row[idx["sell"]] <= bid:
                    transactions.append([i, row[idx["timestamp"]], +1, bid, 0, 1])  # [i, timestamp, side, price, trs_type, otype]
                    pos += 1

    return transactions


def run_dev_mm_bb_v2(
    data: pd.DataFrame,  # ['timestamp', 'ask', 'bid', 'mid', 'buy', 'sell', 'best_bid', 'best_ask', 'dev_ma', 'dev_smooth', 'vol', 'ma_sign']
    horizon=12 * 10,
    threshold=5,
    exit_spread=0,
    use_best_price_for_exit=True,
    sl_level=20,
    tp_level=20,
    vol_threshold=30,
    decimal=0,
):

    df = data.reset_index().copy()
    df.timestamp = df.timestamp.values.astype(float)
    # extract colname -> i
    idx = NDict.empty(key_type=types.unicode_type, value_type=types.int64)
    for i, c in enumerate(df.columns):
        idx[c] = i
    # run
    trs = _run_dev_mm_bb(
        data=df.values.astype(float), idx=idx, threshold=threshold,
        use_best_price_for_exit=use_best_price_for_exit, horizon=horizon, exit_spread=exit_spread, sl_level=sl_level, tp_level=tp_level, vol_threshold=vol_threshold, decimal=decimal,
    )
    trs = pd.DataFrame(trs, columns=["i", "timestamp", "side", "price", "trs_type", "otype"])
    trs.timestamp = pd.to_datetime(trs.timestamp)
    trs.trs_type = trs.trs_type.replace({0: "entry", 1: "LE", 2: "SL", 3: "TP", 4: "HR"})
    trs.otype = trs.otype.replace({0: "market", 1: "limit"})
    # cleansing
    trs = pd.concat([
        trs.iloc[::2].reset_index(drop=True).rename(lambda x: f"entry_{x}", axis=1),
        trs.iloc[1::2].reset_index(drop=True).rename(lambda x: f"exit_{x}", axis=1),
    ], axis=1)
    trs["horizon"] = (trs.exit_timestamp - trs.entry_timestamp).dt.total_seconds()
    trs["pl_raw"] = (trs.exit_price / trs.entry_price).sub(1).mul(1e4).mul(trs.entry_side)
    trs["fee"] = trs.entry_otype.replace({"market": -12, "limit": +2}) + trs.exit_otype.replace({"market": -12, "limit": +2})
    trs["pl"] = trs.pl_raw + trs.fee
    return trs
