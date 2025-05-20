import numpy as np
import pandas as pd
import json
from pathlib import Path
from multiprocessing import Process
from typing import Optional
from glob import glob
from tqdm.auto import tqdm
from tabulate import tabulate
from etr.data.data_loader import list_hdb
import time

from etr.config import Config
from etr.common.logger import LoggerFactory


class HdbDumper:

    table_list = {
        # FH Class Name -> _data_type
        "BitmexSocketClient": ["Rate", "MarketBook", "MarketTrade"],
        "BitBankSocketClient": ["Rate", "MarketBook", "MarketTrade", "CircuitBreaker"],
        "BitFlyerSocketClient": ["Rate", "MarketBook", "MarketTrade"],
        "GmoForexSocketClient": ["Rate"],
        "GmoCryptSocketClient": ["Rate", "MarketBook", "MarketTrade"],
        "CoincheckSocketClient": ["MarketTrade", "Rate", "MarketBook"],
        "BitFlyerFundingRate": ["FundingRate"],
    }

    def __init__(self, hdb_dir: str = Config.HDB_DIR, tp_dir: str = Config.TP_DIR):
        self.hdb_dir = Path(hdb_dir)
        self.tp_dir = Path(tp_dir)
        self.logger = LoggerFactory().get_logger(logger_name=self.__class__.__name__, log_file=Path(Config.LOG_DIR).joinpath("hdb_dumper.log"))
        self.process: Optional[Process] = None

    def dump_to_hdb(self, log_file: Path, skip_if_exists: bool = True) -> None:

        self.logger.info(f"Start extraction for '{Path(log_file).name}'")
        # inspect first 100 lines
        logger_name = Path(log_file).name.split("-")[1]
        date = Path(log_file).name.split(".")[-1]
        sym_from_fname = Path(log_file).name.split("-")[2].split(".log")[0].replace("_", "").upper()
        first_records = []
        with open(log_file, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                json_part = line.split("||", 1)[-1]
                data = json.loads(json_part)
                first_records.append([data.get("venue"), data.get("sym")])
                if i > 100:
                    break
        first_records = pd.DataFrame(first_records, columns=["venue", "sym"]).drop_duplicates().assign(sym=sym_from_fname)
        first_records = first_records.dropna()
        if Path(log_file).name.startswith("TP-GmoCrypt"):  # [NOTE] WA for FH bug found fixed at 2025/05/08
            first_records["venue"] = "gmo"
            first_records = first_records.drop_duplicates()
            self.logger.info("[WA] overwrite venue as 'gmo'")

        # check existing files
        exists_all = True
        for venue, sym in first_records.values:
            for table in self.table_list[logger_name]:
                path = self.build_path(table, date, venue, sym)
                self.logger.info(f"{path.exists()} -- {path}")
                exists_all = exists_all and path.exists()
        if exists_all and skip_if_exists:
            self.logger.info(f"Files are ready for '{Path(log_file).name}', skip processing")
            return

        # Proceed to extraction
        records = {table: [] for table in self.table_list[logger_name]}
        with open(log_file, "r", encoding="utf-8") as f:
            for i, line in tqdm(enumerate(f)):
                json_part = line.split("||", 1)[-1]
                data = json.loads(json_part)
                records[data.get("_data_type").replace("CirbuitBreaker", "CircuitBreaker")].append(data)  # [NOTE] Workaround

        # Read and dump
        for table, lines in records.items():
            if len(lines) == 0:
                self.logger.info(f"No record found for '{table}'")
                continue

            # as DataFrame
            df = pd.DataFrame(lines).drop("_data_type", axis=1)
            for col in df.columns[df.columns.str.contains("time")]:
                df[col] = pd.to_datetime(df[col])
            if "venue" not in df: df["venue"] = venue  # [NOTE] WA for CB table
            if "sym" not in df: df["sym"] = sym_from_fname  # [NOTE] WA for CB table
            if table in ["Rate", "MarketBook"] and Path(log_file).name.startswith("TP-GmoCrypt"):
                # [NOTE] WA for FH bug found fixed at 2025/05/08
                df["venue"] = "gmo"
            for sym, venue in df[["sym", "venue"]].drop_duplicates().values:
                self.logger.info(f"Saving '{table}-{venue}-{sym_from_fname}'")  # [NOTE] WA for CB table
                path = self.build_path(table, date, venue, sym)  # [NOTE] WA for CB table
                subset = df.query("venue==@venue and sym==@sym")
                if subset.shape[0] > 0:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    df.to_parquet(path)
                    self.logger.info(f"Saved '{path}'")
                else:
                    self.logger.info(f"No record, skipped '{path}'")

    def build_path(self, table, date, venue, sym) -> Path:
        ymd = pd.Timestamp(date).strftime("%Y-%m-%d")
        fname = f"{table}_{venue}_{sym}_{date}.parquet"
        return Path(self.hdb_dir).joinpath(f"{table}/{venue}/{ymd}/{sym}/{fname}")

    def list_log_files(self) -> pd.DataFrame:
        # DataFrame[path, fname, logger_name, date]
        files = pd.Series(glob(f"{self.tp_dir}/*.log.*")).to_frame("path")
        if len(files) > 0:
            files["fname"] = files.path.str.split("/").str[-1]
            files["logger_name"] = files.fname.str.split("-").str[1]
            files["date"] = pd.to_datetime(files.fname.str.split(".").str[-1])
            return files.sort_values("date").reset_index(drop=True)
        else:
            return pd.DataFrame(columns=["fname", "logger_name", "date", "path"])

    def dump_all(self, n_days=10, skip_if_exists=True):
        files = self.list_log_files()
        threshold = pd.Timestamp.today() - pd.Timedelta(f"{n_days}D")
        for file in files.query("@threshold < date").path:
            self.dump_to_hdb(file, skip_if_exists)

    def start_hdb_loop(
        self,
        n_days=20,
        trigger_time_utc: str = "01:00",
        subprocess: bool = False,
    ):
        def closure():
            while True:
                self.dump_all(n_days=n_days)

                # logging
                prev_date = pd.Timestamp.today(tz="UTC") - pd.Timedelta("1D")
                files = list_hdb(prev_date)
                if len(files) > 0:
                    status_table = files.assign(flag=1).set_index(["date", "table", "venue", "sym"]).flag.unstack(level=1).replace({np.nan: " ", 1.0: "v"}).sort_index().reset_index()
                    status = tabulate(status_table, headers='keys', tablefmt='plain')
                    self.logger.info(f'''\n{status}''')
                else:
                    self.logger.info(f"No HDB file found for {prev_date}")

                now = pd.Timestamp.today(tz="UTC")
                next_run = now.ceil("1D").replace(hour=int(trigger_time_utc[:2]), minute=int(trigger_time_utc[3:6]))
                total_seconds = (next_run - now).total_seconds()
                self.logger.info(f"Wait until {next_run}, sleep {int(total_seconds / 60)} min")
                time.sleep(total_seconds)

        if subprocess:
            self.process = Process(target=closure)
            self.process.start()
        else:
            closure()


if __name__ == '__main__':
    HdbDumper().start_hdb_loop()
