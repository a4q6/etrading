import pandas as pd
from pathlib import Path
from glob import glob
from tqdm import tqdm
from typing import Union, List
import json

from etr.config import Config
from etr.common.logger import LoggerFactory

logger = LoggerFactory().get_logger(__name__)

VENUE_TO_TP = {
    "bitmex": ["TP-BitmexSocketClient-{}.log"],
    "gmo": ["TP-GmoForexSocketClient-{}.log", "TP-GmoCryptSocketClient-{}.log"],
    "bitflyer": ["TP-BitFlyerSocketClient-{}.log"],
    "coincheck": ["TP-CoincheckSocketClient-{}.log", "TP-CoincheckRestClient-{}.log"],
    "bitbank": ["TP-BitBankSocketClient-{}.log"],
    "binance": ["TP-BinanceSocketClient-{}.log", "TP-BinanceRestEoption-{}.log"]
}


def read_log_ticker(
    data_type: str = "MarketBook",
    log_file="data/tp/TP_BitBankSocketClient.log",
) -> pd.DataFrame:
    # read log ticker
    records = []
    with open(log_file, "r", encoding="utf-8") as f:
        for line in tqdm(f):
            json_part = line.split("||", 1)[-1]
            data = json.loads(json_part)
            _data_type = data.get("_data_type")
            if _data_type == data_type:
                records.append(data)

    # convert as table
    data = pd.DataFrame(records)
    if "_data_type" in data:
        data = data.drop("_data_type", axis=1)
    for col in data.columns[data.columns.str.contains("time")]:
        data[col] = pd.to_datetime(data[col])

    return data


def load_data(
    date: Union[str, List[str]],
    table: str = "MarketTrade",
    venue: str = "*",
    symbol: str = "BTCJPY",
    parent_dir: Path = None
):
    assert table != "*", "wildcard specification for table name is not supported."

    # set parent directory
    if parent_dir is None:
        parent_dir = Path(__file__).parent.parent.parent.parent
    else:
        parent_dir = Path(parent_dir)
    hdb_dir = parent_dir.joinpath(Config.HDB_DIR)
    tp_dir = parent_dir.joinpath(Config.TP_DIR)

    # search files
    if isinstance(date, list) and len(date) == 2:
        dates = pd.date_range(*date).strftime("%Y-%m-%d")
        latest_date = pd.Timestamp(dates[-1])
        files = []
        for ymd in dates:
            files += glob(hdb_dir.joinpath(table).joinpath(venue).joinpath(ymd).joinpath(symbol).joinpath("*.parquet").as_posix())
    else:
        latest_date = pd.Timestamp(date)
        ymd = latest_date.strftime("%Y-%m-%d")
        files = glob(hdb_dir.joinpath(table).joinpath(venue).joinpath(ymd).joinpath(symbol).joinpath("*.parquet").as_posix())

    # read log ticker if necessary
    latest_data = pd.DataFrame()
    if pd.Timestamp.today(tz="UTC").floor("D").tz_convert(None) <= latest_date:
        assert all(arg != "*" for arg in [venue, symbol]), "wildcard specification is not allowed for intraday data."
        logger.info(f"Loading log ticker for ({table}, {venue}, {symbol}) ...")
        tp_names = VENUE_TO_TP.get(venue)
        tp_files = [tp_dir.joinpath(fname.format(symbol)) for fname in tp_names if tp_dir.joinpath(fname.format(symbol)).exists()]
        assert venue is not None, f"TP file of '{venue}' is not available or registered to mapping table."
        latest_data = pd.concat([read_log_ticker(table, log_file=tp_file) for tp_file in tp_files])

    data = pd.DataFrame()
    if len(files) > 0:
        logger.info(f"Found {len(files)} HDB files, process loading...")
        fragments = []
        for file in tqdm(files):
            df = pd.read_parquet(file)
            # convert tz as UTC
            for column in df:
                if "time" in column:
                    if hasattr(df[column], "dt"):
                        if df[column].dt.tz is not None:
                            df[column] = df[column].dt.tz_convert("UTC")
                        else:
                            df[column] = df[column].dt.tz_localize("UTC")
            fragments.append(df)
        data = pd.concat(fragments)
    data = pd.concat([data, latest_data])

    return data


def list_hdb(
    date: str,
    parent_dir: Path = None
) -> pd.DataFrame:
    # set parent directory
    if parent_dir is None:
        parent_dir = Path(__file__).parent.parent.parent.parent
    else:
        parent_dir = Path(parent_dir)
    hdb_dir = parent_dir.joinpath(Config.HDB_DIR)

    ymd = pd.Timestamp(date).strftime("%Y-%m-%d")
    files = pd.Series([path.as_posix() for path in hdb_dir.glob(f"*/*/{ymd}/*/*.parquet")], name="path")  # table/venue/date/sym/file.parquet
    if len(files) > 0:
        partition = files.astype(str).str.split("/")
        files_df = files.to_frame()
        files_df["sym"] = partition.str[-2]
        files_df["date"] = pd.to_datetime(partition.str[-3])
        files_df["venue"] = partition.str[-4]
        files_df["table"] = partition.str[-5]
        files_df = files_df[["table", "venue", "sym", "date", "path"]]
        return files_df
    else:
        return pd.DataFrame()
