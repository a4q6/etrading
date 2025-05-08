import json
import requests
import itertools
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from tqdm.auto import tqdm
from etr.data.gmo_historical import save
from etr.config import Config


if __name__ == "__main__":
    symbols = ["BTC", "BTC_JPY", "ETH", "ETH_JPY", "XRP", "XRP_JPY", "LTC", "LTC_JPY"]
    months = set([d.replace(day=1) for d in pd.date_range("2020-01-01", "2024-04-01")])
    args_list = [(month, sym) for month, sym in itertools.product(months, symbols)]

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(
            tqdm(
                executor.map(lambda args: save(*args, parent=Path(Config.HDB_DIR).parent.joinpath("gmo")), args_list),
                total=len(args_list),
            )
        )
