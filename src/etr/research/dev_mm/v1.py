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
def _run_dev_mm(
    data: np.ndarray,
    idx: NDict,
    horizon=12 * 10,
    use_best_price_for_exit=False,
    exit_spread=3,
    sl_level=20,
    tp_level=20,
    vol_threshold=30,
    decimal=0,
):
    # run simulation
    pos = 0
    transactions = []
    for i, row in enumerate(data):
        if pos != 0:
            # LIMIT EXIT
            if pos > 0:
                price = row[idx["best_ask"]] if use_best_price_for_exit else row[idx["mid"]]
                price = my_ceil(price * (1 + exit_spread / 1e4), n=decimal)
                if price < row[idx["buy"]]:
                    transactions.append([i, row[idx["timestamp"]], -1, price, 1, 1])  # [i, timestamp, side, price, trs_type, otype]
                    pos = 0
            elif pos < 0:
                price = row[idx["best_bid"]] if use_best_price_for_exit else row[idx["mid"]]
                price = my_floor(price * (1 - exit_spread / 1e4), n=decimal)
                if price > row[idx["sell"]]:
                    transactions.append([i, row[idx["timestamp"]], +1, price, 1, 1])  # [i, timestamp, side, price, trs_type, otype]
                    pos = 0

            # SL
            if pos != 0 and pos * (row[idx["mid"]] / transactions[-1][-3] - 1) * 1e4 < -sl_level:
                price = row[idx["best_bid"]] if pos > 0 else row[idx["best_ask"]]
                transactions.append([i, row[idx["timestamp"]], pos * -1, price, 2, 0])  # [i, timestamp, side, price, trs_type, otype]
                pos = 0

            # TP
            price = row[idx["best_bid"]] if pos > 0 else row[idx["best_ask"]]
            if pos != 0 and pos * (price / transactions[-1][-3] - 1) * 1e4 > tp_level:
                transactions.append([i, row[idx["timestamp"]], pos * -1, price, 3, 0])  # [i, timestamp, side, price, trs_type, otype]
                pos = 0

            # HORIZON
            if pos != 0 and transactions[-1][0] + horizon < i:
                price = row[idx["best_bid"]] if pos > 0 else row[idx["best_ask"]]
                transactions.append([i, row[idx["timestamp"]], pos * -1, price, 4, 0])  # [i, timestamp, side, price, trs_type, otype]
                pos = 0
        else:
            if row[idx["vol"]] < vol_threshold:
                if row[idx["dev_ma"]] > 0 and row[idx["ma_sign"]] < 0:
                    ask = my_ceil(np.maximum(row[idx["ask"]], row[idx["mid"]]), n=decimal)
                    if ask < row[idx["buy"]]:
                        transactions.append([i, row[idx["timestamp"]], -1, ask, 0, 1])  # [i, timestamp, side, price, trs_type, otype]
                        pos -= 1
                elif row[idx["dev_ma"]] < 0 and row[idx["ma_sign"]] > 0:
                    bid = my_floor(np.minimum(row[idx["bid"]], row[idx["mid"]]), n=decimal)
                    if row[idx["sell"]] < bid:
                        transactions.append([i, row[idx["timestamp"]], +1, bid, 0, 1])  # [i, timestamp, side, price, trs_type, otype]
                        pos += 1

    return transactions


def run_dev_mm(
    data: pd.DataFrame,  # ['timestamp', 'ask', 'bid', 'mid', 'buy', 'sell', 'best_bid', 'best_ask', 'dev_ma', 'dev_smooth', 'vol', 'ma_sign']
    horizon=12 * 10,
    exit_spread=3,
    use_best_price_for_exit=False,
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
    trs = _run_dev_mm(
        data=df.values.astype(float), idx=idx,
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
    trs["pl_raw"] = (trs.exit_price / trs.entry_price).sub(1).mul(1e4).mul(trs.entry_side)
    return trs

@numba.jit(nopython=False)
def _run_dev_mm_2(
    data: np.ndarray,
    idx: NDict,
    horizon=12 * 10,
    use_best_price_for_exit=False,
    exit_spread=3,
    sl_level=20,
    tp_level=20,
    vol_threshold=30,
    decimal=0,
):
    # run simulation
    pos = 0
    transactions = []
    for i, row in enumerate(data):
        if pos != 0:
            # HORIZON - LE
            if pos != 0 and transactions[-1][0] + horizon < i:
                # LIMIT EXIT
                if pos > 0:
                    price = row[idx["best_ask"]] if use_best_price_for_exit else row[idx["mid"]]
                    price = my_ceil(price * (1 + exit_spread / 1e4), n=decimal)
                    if price < row[idx["buy"]]:
                        transactions.append([i, row[idx["timestamp"]], -1, price, 1, 1])  # [i, timestamp, side, price, trs_type, otype]
                        pos = 0
                elif pos < 0:
                    price = row[idx["best_bid"]] if use_best_price_for_exit else row[idx["mid"]]
                    price = my_floor(price * (1 - exit_spread / 1e4), n=decimal)
                    if price > row[idx["sell"]]:
                        transactions.append([i, row[idx["timestamp"]], +1, price, 1, 1])  # [i, timestamp, side, price, trs_type, otype]
                        pos = 0

            # SL
            if pos != 0 and pos * (row[idx["mid"]] / transactions[-1][-3] - 1) * 1e4 < -sl_level:
                price = row[idx["best_bid"]] if pos > 0 else row[idx["best_ask"]]
                transactions.append([i, row[idx["timestamp"]], pos * -1, price, 2, 0])  # [i, timestamp, side, price, trs_type, otype]
                pos = 0

            # TP
            price = row[idx["best_bid"]] if pos > 0 else row[idx["best_ask"]]
            if pos != 0 and pos * (price / transactions[-1][-3] - 1) * 1e4 > tp_level:
                transactions.append([i, row[idx["timestamp"]], pos * -1, price, 3, 0])  # [i, timestamp, side, price, trs_type, otype]
                pos = 0
        else:
            if row[idx["vol"]] > vol_threshold:
                # if row[idx["dev_ma"]] > 0 and row[idx["ma_sign"]] < 0:
                if True:
                    ask = my_ceil(np.maximum(row[idx["ask"]], row[idx["mid"]]), n=decimal)
                    if ask < row[idx["buy"]]:
                        transactions.append([i, row[idx["timestamp"]], -1, ask, 0, 1])  # [i, timestamp, side, price, trs_type, otype]
                        pos -= 1
                # elif row[idx["dev_ma"]] < 0 and row[idx["ma_sign"]] > 0:
                if True:
                    bid = my_floor(np.minimum(row[idx["bid"]], row[idx["mid"]]), n=decimal)
                    if row[idx["sell"]] < bid:
                        transactions.append([i, row[idx["timestamp"]], +1, bid, 0, 1])  # [i, timestamp, side, price, trs_type, otype]
                        pos += 1

    return transactions


def run_dev_mm_2(
    data: pd.DataFrame,  # ['timestamp', 'ask', 'bid', 'mid', 'buy', 'sell', 'best_bid', 'best_ask', 'dev_ma', 'dev_smooth', 'vol', 'ma_sign']
    horizon=12 * 10,
    exit_spread=3,
    use_best_price_for_exit=False,
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
    trs = _run_dev_mm_2(
        data=df.values.astype(float), idx=idx,
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
    trs["pl_raw"] = (trs.exit_price / trs.entry_price).sub(1).mul(1e4).mul(trs.entry_side)
    return trs
