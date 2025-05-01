import asyncio
import websockets
import json
import numpy as np
import datetime
import pytz
import pandas as pd
from typing import Callable, Awaitable, Optional, List, Dict
from sortedcontainers import SortedDict
from uuid import uuid4
from copy import deepcopy
from pathlib import Path

from etr.core.async_logger import AsyncBufferedLogger
from etr.config import Config
from etr.core.datamodel import MarketBook, MarketTrade, Rate, VENUE
from etr.common.logger import LoggerFactory


class GmoForexSocketClient:
    def __init__(
        self,
        ccy_pairs: List[str] = ["USD_JPY", "EUR_USD", "GBP_USD", "AUD_USD", "EUR_JPY", "GBP_JPY", "CHF_JPY", "CAD_JPY", "AUD_JPY"],
        callbacks: List[Callable[[dict], Awaitable[None]]] = [],
        reconnect_attempts: Optional[int] = None,  # no limit
    ):
        self.ws_url = "wss://forex-api.coin.z.com/ws/public/v1"
        self.callbacks = callbacks

        # logger
        log_file = Path(Config.LOG_DIR).joinpath("main.log").as_posix()
        tp_file = Path(Config.TP_DIR)
        self.ticker_plant: Dict[str, AsyncBufferedLogger] = {
            ccy_pair: AsyncBufferedLogger(logger_name=f"TP-{self.__class__.__name__}-{ccy_pair}", log_dir=tp_file.as_posix())
            for ccy_pair in ccy_pairs
        }
        self.logger = LoggerFactory().get_logger(logger_name="main", log_file=log_file)
        
        # status flags
        self._ws = None
        self._connected = False
        self._running = True
        self.reconnect_attempts = reconnect_attempts

        # channels
        self.channels = []
        self.market_book: Dict[str, MarketBook] = {}
        self.rate: Dict[str, Rate] = {}
        self.diff_message_buffer: Dict[str, SortedDict] = {}
        self.heatbeat_memo = datetime.datetime.now(datetime.timezone.utc)
        for ccy_pair in ccy_pairs:
            self.channels.append(f"{ccy_pair}")
            # self.market_book[ccy_pair] = MarketBook(sym=ccy_pair.replace("_", "").upper(), venue=VENUE.BITFLYER, category="json-rpc", misc="null")
            self.rate[ccy_pair] = Rate(sym=ccy_pair.replace("_", "").upper(), venue=VENUE.GMO, category="websocket", timestamp=datetime.datetime.now(datetime.timezone.utc))
            # self.diff_message_buffer[ccy_pair] = SortedDict()
            # self.last_emit_market_book[ccy_pair] = datetime.datetime(2000, 1, 1, tzinfo=pytz.timezone("UTC"))

    async def start(self):
        self.attempts = 0
        while self._running:
            if self.reconnect_attempts is not None and self.attempts >= self.reconnect_attempts:
                self.logger.error("Reached max connection attempts, stop listening.")
                break
            try:
                await self._connect()
            except Exception as e:
                self.attempts += 1
                self.logger.error(f"Connection Error (#Attempts={self.attempts}): {e}", exc_info=True)
                sleep_sec = 10 * np.log(self.attempts) + GmoForexSocketClient.sleep_offset()
                self.logger.info(f"Wait {round(sleep_sec, 2)} seconds to reconnect...")
                await asyncio.sleep(sleep_sec)

    async def _connect(self):
        async with websockets.connect(self.ws_url) as websocket:
            self._connected = True
            self._ws = websocket
            self.logger.info(f"Start subscribing: {self.ws_url}")

            # Send request
            for channel in self.channels:
                subscribe_message = {"command": "subscribe", "channel": "ticker", "symbol": channel}
                await websocket.send(json.dumps(subscribe_message))
                self.logger.info(f"Send request for '{subscribe_message}'")
                await asyncio.sleep(2)

            try:
                while self._connected and self._running:
                    raw_msg = await websocket.recv()
                    message = json.loads(raw_msg)
                    await self._process_message(message)
                    self.attempts = 0  # reset retry counts

            except websockets.exceptions.ConnectionClosedOK:
                self.logger.info("Websoket closed OK")
            except Exception as e:
                self.logger.error(f"Websocket closed ERR: {e}")
                raise
            finally:
                self._connected = False
                self.logger.info("Close websocket")

    async def close(self):
        self._running = False
        if self._ws:
            await self._ws.close()
        for logger in self.ticker_plant.values():
            logger.stop()

    async def _process_message(self, message: dict):
        
        if set(["symbol", "bid", "ask", "timestamp", "status"]) <= set(message.keys()):
            # Rate message  
            # {"symbol":"USD_JPY","ask":"142.674","bid":"142.669","timestamp":"2025-04-30T07:16:14.336395Z","status":"OPEN"}
            ccypair = message["symbol"]
            data = Rate(
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                market_created_timestamp=datetime.datetime.strptime(
                        message["timestamp"][:23].replace("T", " ").replace("Z", ""),
                        "%Y-%m-%d %H:%M:%S.%f",
                    ).replace(tzinfo=pytz.timezone("UTC")),
                sym=ccypair.replace("_", ""),
                venue=VENUE.GMO,
                category="websocket",
                best_bid=float(message["bid"]),
                best_ask=float(message["ask"]),
                mid_price=round((float(message["bid"]) + float(message["ask"])) / 2, 7),
                misc=message["status"],
            )
            if self.callbacks: asyncio.create_task(asyncio.gather(*[callback(data) for callback in self.callbacks]))  # send(wo-awaiting)
            if self.rate[ccypair].timestamp + datetime.timedelta(milliseconds=250) < data.timestamp:  # 250ms throttling
                self.rate[ccypair] = data
                asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(data.to_dict()))) # store

            if self.heatbeat_memo + datetime.timedelta(seconds=60) < data.timestamp:
                self.heatbeat_memo = data.timestamp
                last_update = "\n".join([f"{rate.sym}\t{rate.timestamp.isoformat()}" for ccypair, rate in self.rate.items()])
                self.logger.info(f"heartbeat:\n{last_update}")

    @staticmethod
    def sleep_offset() -> float:
        now = pd.Timestamp.now(tz="US/Eastern")
        def is_weekend(now) -> bool:
            if now.weekday() == 4 and 17 <= now.hour:
                return True
            elif now.weekday() == 5:
                return True
            elif now.weekday() == 6 and now.hour <= 17:
                return True
            else:
                return False
        
        if is_weekend(now):
            next_ny18 = now.replace(hour=18, minute=0, second=0, microsecond=0) + pd.Timedelta(f"{6 - now.weekday()}D")
            duration = (next_ny18 - now).total_seconds()
            return duration
        else:
            return 0

if __name__ == '__main__':
    client = GmoForexSocketClient(ccy_pairs=["USD_JPY", "GBP_USD"])
    try:
        asyncio.run(client.start())
    except KeyboardInterrupt:
        print("Disconnected")
        asyncio.run(client.close())
