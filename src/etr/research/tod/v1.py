import numba
import numpy as np
import pandas as pd
import datetime
from datetime import time
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


def run_tod(
    data,
    side=1,
    st=time(16, 20),
    et=time(20, 30),
    entry_duration=5,
    exit_duration=5,
    entry_offset=1,
    exit_offset=1,
    sl_level=20,
    tp_level=50,
    decimal=3,
):
    # extract target timebars
    add_timedelta = lambda x, duration: (datetime.datetime.combine(datetime.datetime(2000, 1, 1), x) + pd.Timedelta(duration)).time()
    target_timebars = data.between_time(st, et).index
    entry_et = add_timedelta(st, f"{entry_duration}min")
    entry_bars = data.between_time(st, entry_et).index
    exit_st = add_timedelta(et, f"-{exit_duration}min")
    exit_bars = data.between_time(exit_st, et).index
    # add parameter info
    df = data.copy()
    df["target_bars"] = df.index.isin(target_timebars)
    df["entry_bars"] = df.index.isin(entry_bars) if entry_duration > 0 else False
    df["exit_bars"] = df.index.isin(exit_bars) if exit_duration > 0 else False
    df["timestamp"] = df.index.values.astype(float)
    # NumbaDict
    idx = NDict.empty(key_type=types.unicode_type, value_type=types.int64)
    for i, c in enumerate(df.columns):
        idx[c] = i
    # Run
    trs = _run_tod(df.values.astype(float), idx, side=side, entry_offset=entry_offset, exit_offset=exit_offset, sl_level=sl_level, tp_level=tp_level, decimal=decimal)
    trs = pd.DataFrame(trs, columns=["i", "timestamp", "side", "price", "trs_type", "otype"])
    trs.timestamp = pd.to_datetime(trs.timestamp)
    trs.trs_type = trs.trs_type.replace({0: "LE", 1: "ME", 2: "SL", 3: "LX", 4: "MX"})
    trs.otype = trs.otype.replace({0: "market", 1: "limit"})
    # cleansing
    trs = pd.concat([
        trs.iloc[::2].drop("i", axis=1).reset_index(drop=True).rename(lambda x: f"entry_{x}", axis=1),
        trs.iloc[1::2].drop("i", axis=1).reset_index(drop=True).rename(lambda x: f"exit_{x}", axis=1),
    ], axis=1)
    trs["timestamp"] = trs.entry_timestamp
    trs["pl_raw"] = trs["pl"] = (trs.exit_price / trs.entry_price).sub(1).mul(1e4).mul(trs.entry_side)
    return trs


@numba.njit
def _run_tod(
    array,
    idx: NDict,
    side=1,
    entry_offset=1,
    exit_offset=1,
    sl_level=20,
    tp_level=50,
    spread_cost=4,
    decimal=3,
):
    pos = 0
    stop_price = np.nan
    transactions = []
    entry_flag = False
    for i, row in enumerate(array):
        if pos == 0:
            if row[idx["target_bars"]] > 0 and row[idx["entry_bars"]] > 0 and not entry_flag:
                # ENTRY LIMIT
                price = row[idx["close"]] * (1 - side * (entry_offset / 1e4))
                price = my_floor(price, decimal) if side > 0 else my_ceil(price, decimal)
                ref_price = row[idx["buy_price"]] if side < 0 else row[idx["sell_price"]]
                if (price - ref_price) * side >= 0:
                    transactions.append([i, row[idx["timestamp"]], side, price, 0, 1])  # [i, timestamp, side, price, trs_type, otype]
                    stop_price = transactions[-1][-3] * (1 - side * (sl_level / 1e4))
                    pos = side
                    entry_flag = True

            elif row[idx["target_bars"]] > 0 and not row[idx["exit_bars"]] > 0 and not entry_flag:
                # ENTRY MARKET
                price = round(row[idx["close"]] * (1 + side * (spread_cost / 1e4)), decimal)
                transactions.append([i, row[idx["timestamp"]], side, price, 1, 0])  # [i, timestamp, side, price, trs_type, otype]
                stop_price = transactions[-1][-3] * (1 - side * (sl_level / 1e4))
                pos = side
                entry_flag = True

        elif pos != 0:
            # SL
            new_stop = row[idx["close"]] * (1 - pos * (sl_level / 1e4))
            if (new_stop - stop_price) * pos > 0:
                stop_price = new_stop
            if (row[idx["close"]] - stop_price) * pos < 0:
                price = round(stop_price * (1 - pos * spread_cost / 1e4), decimal)
                transactions.append([i, row[idx["timestamp"]], -side, price, 2, 1])  # [i, timestamp, side, price, trs_type, otype]
                pos = 0
                stop_price = np.nan
                continue

            # EXIT LIMIT
            if pos != 0 and row[idx["target_bars"]] > 0 and row[idx["exit_bars"]] > 0:
                limit_side = -1 * side
                price = row[idx["close"]] * (1 - limit_side * (exit_offset / 1e4))
                price = my_floor(price, decimal) if limit_side > 0 else my_ceil(price, decimal)
                ref_price = row[idx["buy_price"]] if limit_side < 0 else row[idx["sell_price"]]
                if (price - ref_price) * limit_side >= 0:
                    transactions.append([i, row[idx["timestamp"]], -side, price, 3, 1])  # [i, timestamp, side, price, trs_type, otype]
                    pos = 0
                    stop_price = np.nan

            # EXIT MARKET
            if pos != 0 and row[idx["target_bars"]] == 0:
                price = round(row[idx["close"]] * (1 - side * (spread_cost / 1e4)), decimal)
                transactions.append([i, row[idx["timestamp"]], -side, price, 4, 0])  # [i, timestamp, side, price, trs_type, otype]
                pos = 0
                stop_price = np.nan

        if row[idx["target_bars"]] == 0:
            entry_flag = False

    return transactions
