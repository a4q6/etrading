import numpy as np
import pandas as pd
import json
import time
import shutil
from pathlib import Path
from multiprocessing import Process
from typing import Optional, List, Dict
from glob import glob
from tqdm.auto import tqdm
from tabulate import tabulate

from etr.data.data_loader import list_hdb
from etr.core.datamodel import VENUE
from etr.core.notification.discord import send_discord_webhook
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
        "BinanceSocketClient": ["Rate", "MarketTrade"],
        "BinanceRestEoption": ["ImpliedVolatility"],
        "CoincheckRestClient": ["Order", "Trade"],
        "BitBankPrivateStream": ["Order", "Trade", "PositionUpdate"],
        "BitBankPrivateRest": ["Order", "Trade", "PositionUpdate"],
    }
    venue_map = {
        "BitmexSocketClient": VENUE.BITMEX,
        "BitBankSocketClient": VENUE.BITBANK,
        "BitFlyerSocketClient": VENUE.BITFLYER,
        "GmoForexSocketClient": VENUE.GMO,
        "GmoCryptSocketClient": VENUE.GMO,
        "CoincheckSocketClient": VENUE.COINCHECK,
        "BitFlyerFundingRate": VENUE.BITFLYER,
        "BinanceSocketClient": VENUE.BINANCE,
        "BinanceRestEoption": VENUE.BINANCE,
        "CoincheckRestClient": VENUE.COINCHECK,
        "BitBankPrivateStream": VENUE.BITBANK,
        "BitBankPrivateRest": VENUE.BITBANK,
    }
    mtp_pairs = [
        ("BitBankPrivateStream", "BitBankPrivateRest"),
    ]

    def __init__(self, hdb_dir: str = Config.HDB_DIR, tp_dir: str = Config.TP_DIR):
        self.hdb_dir = Path(hdb_dir)
        self.tp_dir = Path(tp_dir)
        self.logger = LoggerFactory().get_logger(logger_name=self.__class__.__name__, log_file=Path(Config.LOG_DIR).joinpath("hdb_dumper.log"))
        self.process: Optional[Process] = None

        # validate MTP config
        for tp_names in self.mtp_pairs:
            # assert have same table list
            table_sets = [set(self.table_list[tp_name]) for tp_name in tp_names]
            assert all(table_sets[0] == sets for sets in table_sets), f"Non-unique table list detected for {tp_names}"
            # assert same venue
            assert len(set(self.venue_map[tp_name] for tp_name in tp_names)) == 1, f"Non-unique venue mapping detected for {tp_names}"

    def dump_to_hdb(self, log_file: Path, skip_if_exists: bool = True) -> None:

        self.logger.info(f"Start extraction for '{Path(log_file).name}'")
        logger_name = Path(log_file).name.split("-")[1]
        date = Path(log_file).name.split(".")[-1]
        sym_from_fname = Path(log_file).name.split("-")[2].split(".log")[0].replace("_", "").upper()

        # check existing files
        exists_all = True
        venue = self.venue_map.get(logger_name)
        for table in self.table_list[logger_name]:
            path = self.build_path(table, date, venue, sym_from_fname)
            self.logger.info(f"{path.exists()} -- {path}")
            exists_all = exists_all and path.exists()
        if exists_all and skip_if_exists:
            self.logger.info(f"Files are ready for '{Path(log_file).name}', skip processing")
            return

        # Proceed to extraction
        # check if MTP or not
        linked_tps = None
        for mtp_pair in self.mtp_pairs:
            if logger_name in mtp_pair:
                linked_tps = mtp_pair
                self.logger.info(f"Found multi TP file: {linked_tps}")
                break

        # read log file
        if linked_tps is None:
            # Single TP
            records = self.read_tp_file(log_file)
        else:
            # Multi TP
            records = self.read_tp_file(log_file)
            for tp_name in linked_tps:
                if tp_name == logger_name:
                    continue
                add_log_file = Path(log_file).as_posix().replace(logger_name, tp_name)
                if Path(add_log_file).exists():
                    add_records = self.read_tp_file(add_log_file)
                    for k, rec in add_records.items():
                        records[k] += rec

        # select out table records and dump
        for table, lines in records.items():
            if len(lines) == 0:
                self.logger.info(f"No record found for '{table}'")
                continue

            # as DataFrame
            df = pd.DataFrame(lines).drop("_data_type", axis=1)
            for col in df.columns[df.columns.str.contains("time")]:
                df[col] = pd.to_datetime(df[col])

            # Case: single file
            self.logger.info(f"Saving '{table}-{venue}-{sym_from_fname}'")
            if df.shape[0] > 0:
                if sym_from_fname != "ALL":
                    assert df.sym.nunique() == 1, f"sym is not unique in {log_file}"
                path = self.build_path(table, date, venue, sym_from_fname)
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

    def roll_old_log_files(self):
        files = glob(f"{self.tp_dir}/*.log")
        today = pd.Timestamp.today(tz="UTC")
        for file in files:
            # read timestamp
            with open(file, "r", encoding="utf-8") as f:
                for i, line in enumerate(f):
                    if len(line) > 10:
                        timestamp = pd.Timestamp(line.split("||", 1)[0])
                        break
            if timestamp.date() < today.date():
                fname = str(file) + timestamp.strftime(".%Y-%m-%d")
                shutil.move(file, fname)
                self.logger.info(f"Renamed '{file}' => '{fname}'")

    def read_tp_file(self, log_file) -> Dict[str, List]:
        logger_name = Path(log_file).name.split("-")[1]
        records = {table: [] for table in self.table_list[logger_name]}
        with open(log_file, "r", encoding="utf-8") as f:
            for i, line in tqdm(enumerate(f)):
                json_part = line.split("||", 1)[-1]
                data = json.loads(json_part)
                records[data.get("_data_type")].append(data)
        return records

    def dump_all(self, n_days=10, skip_if_exists=True):
        self.roll_old_log_files()
        files = self.list_log_files()
        threshold = pd.Timestamp.today() - pd.Timedelta(f"{n_days}D")
        for file in files.query("@threshold < date").path:
            self.dump_to_hdb(file, skip_if_exists)

    def start_hdb_loop(
        self,
        n_days=20,
        trigger_time_utc: str = "01:00",
        subprocess: bool = False,
        notification: bool = False,
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
                    if notification:
                        self.logger.info("Send notification message.")
                        send_discord_webhook("EoD Process Finished!" + "\n" + str(status_table.set_index("date").to_csv(sep="|")), username="EoD-HDB")
                else:
                    self.logger.info(f"No HDB file found for {prev_date}")

                # sleep
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
    HdbDumper().roll_old_log_files()
    # HdbDumper().dump_to_hdb("data/tp/TP-BitBankPrivateStream-ALL.log.2025-06-28")
