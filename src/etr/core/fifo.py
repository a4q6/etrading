import time, datetime
from uuid import uuid4
import numpy as np
import pandas as pd
import json
import itertools
import numba
from dataclasses import dataclass, asdict
from typing import List, Dict, Tuple, Union

from etr.core.datamodel import Trade

EPS = 1e-8


# @numba.jit(nopython=True)
# def _fifo_matching(Q: int, cost: float, transactions: np.ndarray):
#     """ update `Q` and `cost` by new `transactions`.
#     args:
#         Q (int): current amt (long: 0 < Q, short: Q < 0)
#         cost (float): current position evaluated price
#         transactions (np.ndarray[*, 2]): current transactions, [[amount, price]...]
#     Returns:
#         newQ (float): new amt.
#         cost_post (float): new position eval price
#         pnl_closed (float): pnl from current matching
#     """
#     pnl_closed = 0
#     for amount, price in transactions:
#         if Q == 0:
#             # entry new position
#             cost = price
#             Q = amount
#         elif amount * Q > 0:
#             # increase amt in same side
#             cost = ((Q * cost) + (price * amount)) / (Q + amount)
#             Q += amount
#         elif amount * Q < 0:
#             # calc pnl by matching
#             newQ = round(Q + amount, 5)
#             matched_amount = amount if abs(amount) <= abs(Q) else Q
#             pnl_closed += (price - cost) * np.sign(Q) * abs(matched_amount)
#             cost = cost if abs(amount) < abs(Q) else price
#             Q = newQ
#         else:
#             raise ValueError("Unexpected case")

#     if Q == 0:
#         cost = 0

#     return Q, cost, pnl_closed


# @numba.jit(nopython=True)
def _fifo_transaction_matching(transactions: np.ndarray):
    """ do fifo mathcing pnl calculation. """
    # trs: [amount, price, i]
    # pos: [amount, price, i]
    res = []  # List [[rinc_price,rinc_amt,rdec_price,...], ...]
    pos_queue = []
    # res.append(np.array([1., 1., 1.])); res.pop()
    # pos_queue.append(np.array([1., 1., 1.])); pos_queue.pop()
    
    for trs in transactions:
        if (len(pos_queue) == 0) or (np.sign(trs[0]) == np.sign(pos_queue[0][0])):
            # rinc_
            pos_queue.append(trs)
        else:
            # rdec_
            endflag = False
            while not endflag:  # trsが消えるまで
                pos = pos_queue[0]
                if abs(pos[0]) < EPS:
                    raise ValueError()
                if round(abs(pos[0] + trs[0]), 7) < EPS:
                    # equal rdec_
                    res.append(np.append(pos, trs))
                    pos_queue.pop(0)
                    endflag = True
                elif abs(trs[0]) < abs(pos[0]):
                    # partial rdec_
                    res.append(np.append(pos, trs))
                    pos[0] += trs[0]
                    endflag = True
                else:
                    # flip by rdec_.
                    res.append(np.append(pos, trs))
                    trs[0] += pos[0]
                    pos_queue.pop(0)

                    if len(pos_queue) == 0:
                        endflag = True
                        pos_queue.append(trs)
                        
    for pos in pos_queue:
        res.append(np.append(pos, [np.nan, np.nan, np.nan]))

    return res


def calc_matching_table(trades: Union[List[Trade], pd.DataFrame]) -> pd.DataFrame:
    # convert to DataFrame
    if isinstance(trades, List):
        trades_df = pd.DataFrame([asdict(trd) for trd in trades])
        if trades_df.shape[0] == 0:
            return pd.DataFrame()
    else:
        trades_df = trades.copy()
    
    trades_df = trades_df.sort_values("timestamp")
    trades_df["id"] = np.arange(trades_df.shape[0])
    trades_df["sided_amount"] = trades_df[["amount", "side"]].prod(axis=1)
    assert all(trades_df[["venue", "sym", "model_id", "process_id"]].nunique() <= 1),\
        f"This function must be applied to unique (model, sym, venue): {trades_df[['venue', 'sym', 'model_id', 'process_id']].nunique()}. check the #unique value."

    # [main] get matching
    res = _fifo_transaction_matching(trades_df[["sided_amount", "price", "id"]].values)

    ## add columns
    tab = pd.DataFrame(np.array(res), columns=["rinc_amount", "rinc_price", "rinc_id", "rdec_amount", "rdec_price", "rdec_id"])
    for col, side in itertools.product(["timestamp", "order_id", "trade_id", "order_type"], ["rinc", "rdec"]):
        tab = pd.merge(
            tab, 
            trades_df[["id", col]].rename({col: f"{side}_{col}"}, axis=1), 
            left_on=f"{side}_id", right_on="id", how="left"
        ).drop(["id"], axis=1)

    tab["amount"] = tab[["rinc_amount", "rdec_amount"]].abs().min(axis=1)
    tab["side"] = np.sign(tab.rinc_amount)
    tab["pl_bp"] = (tab.rdec_price / tab.rinc_price - 1) * tab.side * 1e4
    tab["pl"] = tab.pl_bp * tab.amount
    tab["sym"] = trades_df.sym.iloc[0]
    tab["horizon"] = (tab.rdec_timestamp - tab.rinc_timestamp).dt.total_seconds()
    
    # add position column
    pos = pd.concat([
        tab[["rinc_timestamp", "amount", "side"]].rename({"rinc_timestamp": "timestamp"}, axis=1).set_index("timestamp").prod(axis=1),
        tab[["rdec_timestamp", "amount", "side"]].rename({"rdec_timestamp": "timestamp"}, axis=1).set_index("timestamp").prod(axis=1).mul(-1)
    ]).sort_index().rename("position").reset_index().dropna().set_index("timestamp").position.cumsum()
    
    tab["pos_before_rinc"] = pos.asof(tab.rinc_timestamp).values - tab[["amount", "side"]].prod(axis=1).values
    tab["pos_before_rdec"] = pos.asof(tab.rdec_timestamp).values + tab[["amount", "side"]].prod(axis=1).values
    
    # remove cols
    tab = tab.drop(["rdec_amount", "rinc_amount", "rdec_id", "rinc_id"], axis=1)
    
    # alignment
    tab = tab[[
        "rinc_timestamp", "rdec_timestamp", "sym", "side", "rinc_price", "rdec_price", "pl_bp", "amount", "pl", "horizon", 
        "pos_before_rinc", "pos_before_rdec", "rinc_order_type", "rdec_order_type", "rinc_order_id", "rdec_order_id", "rinc_trade_id", "rdec_trade_id"
    ]].rename({"rinc_timestamp": "timestamp"}, axis=1)
    
    # attach attrs
    tab.attrs = {"venue": trades_df.venue.iloc[0], "model_id": trades_df.model_id.iloc[0], "process_id": trades_df.process_id.iloc[0]}
    
    return tab
