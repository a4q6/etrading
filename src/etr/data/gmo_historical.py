import json
import requests
import itertools
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from tqdm.auto import tqdm


def url(date, sym): 
    d = pd.Timestamp(date)
    ymd = d.strftime("%Y%m%d")
    return f"https://api.coin.z.com/data/trades/{sym}/{d.year}/{d.month:02d}/{ymd}_{sym}.csv.gz"

def get(date, sym):
    df = pd.DataFrame()
    try:
        df = pd.read_csv(url(date, sym))
        df.timestamp = pd.to_datetime(df.timestamp)
    except KeyboardInterrupt:
        raise KeyboardInterrupt
    except:
        pass
    return df

def get_month(month, sym):
    d = pd.Timestamp(month).replace(day=1)
    dates = pd.date_range(d, d + pd.offsets.MonthEnd())
    df = pd.concat(get(date, sym) for date in dates)
    return df

def save(month, sym, parent=Path("../data/gmo"), skip_if_exists=True):
    ym = pd.Timestamp(month).strftime("%Y-%m")
    filepath = parent.joinpath(sym).joinpath(f"{ym}_{sym}.parquet")
    filepath.parent.mkdir(parents=True, exist_ok=True)
    if filepath.exists() and skip_if_exists:
        return
    df = get_month(month, sym)
    if df.shape[0] > 0:
        df.to_parquet(filepath)
