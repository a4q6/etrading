import pandas as pd
import json
from tqdm.auto import tqdm


def read_ticker_plant(log_file="../data/TP_BitBankSocketClient.log"):
    data_by_type = {
        "MarketBook": [],
        "Rate": [],
        "MarketTrade": [],
    }
    with open(log_file, "r", encoding="utf-8") as f:
        for line in tqdm(f):
            json_part = line.split("||", 1)[-1]
            data = json.loads(json_part)
            data_type = data.get("_data_type")
            if data_type in data_by_type:
                data_by_type[data_type].append(data)
    
    # convert as table
    book = pd.DataFrame(data_by_type["MarketBook"]).drop("_data_type", axis=1)
    trades = pd.DataFrame(data_by_type["MarketTrade"]).drop("_data_type", axis=1)
    rates = pd.DataFrame(data_by_type["Rate"]).drop("_data_type", axis=1)
    for df in [book, trades, rates]:
        for col in df.columns[df.columns.str.contains("time")]:
            df[col] = pd.to_datetime(df[col])

    return [rates, book, trades]
