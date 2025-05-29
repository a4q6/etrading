import re
import asyncio
import aiohttp
import time
import datetime
import pandas as pd
import json
from typing import List, Dict, Callable, Awaitable, Optional
from pathlib import Path

from etr.common.utils import camel_to_snake
from etr.common.logger import LoggerFactory
from etr.core.async_logger import AsyncBufferedLogger
from etr.config import Config


class BinanceRestEoption:

    REST_BASE = "https://api.binance.com"
    BASE_URL = "https://eapi.binance.com"

    def __init__(
        self,
        ccy_pairs: List[str] = ["BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "DOGEUSDT"],
        callbacks: List[Callable[[dict], Awaitable[None]]] = [],
        polling_interval = 60,
        reconnect_attempts: Optional[int] = None,  # no limit
    ):
        self.polling_interval = polling_interval
        self.ccy_pairs = ccy_pairs
        self.callbacks = callbacks
        self._running = True

        # logger
        log_file = Path(Config.LOG_DIR).joinpath("main.log").as_posix()
        tp_file = Path(Config.TP_DIR)
        self.ticker_plant: Dict[str, AsyncBufferedLogger] = {
            ccy_pair: AsyncBufferedLogger(logger_name=f"TP-{self.__class__.__name__}-{ccy_pair.upper().replace('_', '')}", log_dir=tp_file.as_posix())
            for ccy_pair in ccy_pairs
        }
        self.logger = LoggerFactory().get_logger(logger_name="main", log_file=log_file)


    async def fetch_spot_prices(self, session):
        url = f"{BinanceRestEoption.REST_BASE}/api/v3/ticker/price"
        try:
            async with session.get(url) as resp:
                data = await resp.json()
                price_dict = {}
                for symbol in self.ccy_pairs:
                    for d in data:
                        if d["symbol"] == symbol:
                            price_dict[symbol] = float(d["price"])
                            break
                return price_dict
        except Exception as e:
            self.logger.error(f"Error while fetching spot data: {e}", exc_info=True)
            return pd.DataFrame()


    async def fetch_iv_data(self, session) -> pd.DataFrame:
        url = f"{self.BASE_URL}/eapi/v1/mark"
        try:
            async with session.get(url) as resp:
                data = await resp.json()
                df = pd.DataFrame(data).rename(camel_to_snake, axis=1)
                df["timestamp"] = datetime.datetime.now(tz=datetime.timezone.utc)
                df["venue"] = "binance"
                df["category"] = "REST"
                symbols = df.symbol.str.split("-")
                df["sym"] = symbols.str[0] + "USDT"
                df["expiry_time"] = pd.to_datetime(symbols.str[1], format="%y%m%d").add(pd.Timedelta("8h")).dt.tz_localize("UTC")
                df["expiry"] = (df.expiry_time - df.timestamp).dt.total_seconds() / 60
                df["strike"] = symbols.str[2].astype(float)
                df["ex_type"] = symbols.str[-1]
                c = ["mark_price", "bid_iv", "ask_iv", "mark_iv", "delta", "theta", "gamma", "vega", "high_price_limit", "low_price_limit", "risk_free_interest"]
                df[c] = df[c].astype(float)
                order = [
                    'timestamp', 'sym', 'venue', 'category', 'expiry_time', 'strike', 'ex_type', 'symbol',
                    'expiry', 'mark_price', 'bid_iv', 'ask_iv', 'mark_iv', 'delta', 'theta', 'gamma', 'vega', 
                    'high_price_limit', 'low_price_limit', 'risk_free_interest', 
                ]
                df = df[order]
                return df.query("sym in @self.ccy_pairs")
        except Exception as e:
            self.logger.error(f"Error while fetching IV data: {e}", exc_info=True)
            return pd.DataFrame()


    async def start(self):
        while self._running:
            self.logger.info("Try fetching IV surface")
            async with aiohttp.ClientSession() as session:
                spot = await self.fetch_spot_prices(session)
                iv = await self.fetch_iv_data(session)
                if len(spot) > 0 and len(iv) > 0:
                    iv["spot"] = pd.Series(spot).reindex(iv.sym).values
                    iv["_data_type"] = "ImpliedVolatility"
                    if self.callbacks: asyncio.create_task(asyncio.gather(*[callback(iv) for callback in self.callbacks]))  # send(wo-awaiting)
                    d = iv.to_dict(orient="records")
                    for record in d:
                        record["timestamp"] = record["timestamp"].isoformat()
                        record["expiry_time"] = record["expiry_time"].isoformat()
                        asyncio.create_task(self.ticker_plant[record["sym"]].info(json.dumps(record))) # store
                    now = pd.Timestamp.now(tz="UTC")
                    next_time = now.ceil(f"{self.polling_interval}s")
                    sleep_duration = (next_time - now).total_seconds() - 0.5
                else:
                    sleep_duration = 1

            await asyncio.sleep(sleep_duration)


    async def close(self):
        self._running = False
        for logger in self.ticker_plant.values():
            logger.stop()


if __name__ == "__main__":
    client = BinanceRestEoption()
    try:
        asyncio.run(client.start())
    except KeyboardInterrupt:
        print("Disconnected")
        asyncio.run(client.close())
