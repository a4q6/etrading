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


class GmoCryptSocketClient:
    def __init__(
        self,
        ccy_pairs: List[str] = ["BTC", "ETH", "XRP", "LTC", "DOGE", "SOL", "BTC_JPY", "ETH_JPY", "XRP_JPY", "LTC_JPY", "DOGE_JPY", "SOL_JPY"],
        callbacks: List[Callable[[dict], Awaitable[None]]] = [],
        reconnect_attempts: Optional[int] = None,  # no limit
    ):
        self.ws_url = "wss://api.coin.z.com/ws/public/v1"
        self.callbacks = callbacks

        # logger
        log_file = Path(Config.LOG_DIR).joinpath("main.log").as_posix()
        tp_file = Path(Config.TP_DIR)
        self.ticker_plant: Dict[str, AsyncBufferedLogger] = {
            ccy_pair: AsyncBufferedLogger(logger_name=f"TP-{self.__class__.__name__}-{ccy_pair.upper().replace('_', '')}", log_dir=tp_file.as_posix())
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
        self.last_emit_market_book: Dict[str, datetime.datetime] = {}
        self.heatbeat_memo = datetime.datetime.now(datetime.timezone.utc)
        for ccy_pair in ccy_pairs:
            self.channels.append(f"{ccy_pair}")
            self.market_book[ccy_pair] = MarketBook(sym=ccy_pair.replace("_", "").upper(), venue=VENUE.GMO, category="json-rpc", misc="null")
            self.rate[ccy_pair] = Rate(sym=ccy_pair.replace("_", "").upper(), venue=VENUE.GMO, category="websocket", timestamp=datetime.datetime.now(datetime.timezone.utc))
            self.last_emit_market_book[ccy_pair] = datetime.datetime(2000, 1, 1, tzinfo=pytz.timezone("UTC"))

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
                sleep_sec = 10 * np.log(self.attempts)
                self.logger.info(f"Wait {round(sleep_sec, 2)} seconds to reconnect...")
                await asyncio.sleep(sleep_sec)

    async def _connect(self):
        async with websockets.connect(self.ws_url) as websocket:
            self._connected = True
            self._ws = websocket
            self.logger.info(f"Start subscribing: {self.ws_url}")

            # Send request
            for sym in self.channels:
                for channel in ["orderbooks", "trades"]:
                    subscribe_message = {"command": "subscribe", "channel": channel, "symbol": sym}
                    await websocket.send(json.dumps(subscribe_message))
                    self.logger.info(f"Send request for '{subscribe_message}'")
                    await asyncio.sleep(1.5)
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
        
        if "symbol" not in message.keys() or "channel" not in message.keys():
            self.logger.warning(f"Unknown message type: {message}")
            return
        
        # folk by channel type
        ccypair = message["symbol"]
        if message["channel"] == "trades":
            # {"channel":"trades","price":"13492957","side":"SELL","size":"0.004","symbol":"BTC_JPY","timestamp":"2025-04-30T13:09:43.601Z"}
            data = MarketTrade(
                timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
                market_created_timestamp=datetime.datetime.strptime(
                    message["timestamp"][:23].replace("T", " ").replace("Z", ""),
                    "%Y-%m-%d %H:%M:%S.%f",
                ).replace(tzinfo=pytz.timezone("UTC")),
                sym=ccypair.replace("_", ""),
                venue=VENUE.GMO,
                category="websocket",
                side=1 * (message["side"] == "BUY") - 1 * (message["side"] == "SELL"),
                price=float(message["price"]),
                amount=float(message["size"]),
            )
            if self.callbacks: asyncio.create_task(asyncio.gather(*[callback(data) for callback in self.callbacks]))  # send(wo-awaiting)
            asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(data.to_dict()))) # store

        if message["channel"] == "orderbooks":
            # {"channel":"orderbooks","asks":[{"price":"13494275","size":"0.003"},{"price":"13495936","size":"0.01"},{"price":"13495944","size":"0.007"},{"price":"13496209","size":"0.02"},{"price":"13496321","size":"0.02"},{"price":"13496443","size":"0.01"},{"price":"13496711","size":"0.02"},{"price":"13496877","size":"0.02"},{"price":"13498007","size":"0.117"},{"price":"13498087","size":"0.04"},{"price":"13498220","size":"0.02"},{"price":"13498483","size":"0.082"},{"price":"13498749","size":"0.01"},{"price":"13499143","size":"0.009"},{"price":"13499190","size":"0.033"},{"price":"13499946","size":"0.003"},{"price":"13500610","size":"0.01"},{"price":"13500674","size":"0.007"},{"price":"13501403","size":"0.01"},{"price":"13501479","size":"0.04"},{"price":"13501715","size":"0.111"},{"price":"13502484","size":"0.003"},{"price":"13502555","size":"0.001"},{"price":"13502812","size":"0.01"},{"price":"13502953","size":"0.032"},{"price":"13503096","size":"0.02"},{"price":"13503136","size":"0.007"},{"price":"13503412","size":"0.03"},{"price":"13505854","size":"0.202"},{"price":"13506017","size":"0.03"}],"bids":[{"price":"13491909","size":"0.049"},{"price":"13491082","size":"0.025"},{"price":"13490000","size":"0.1"},{"price":"13488616","size":"0.02"},{"price":"13488325","size":"0.007"},{"price":"13488269","size":"0.02"},{"price":"13488253","size":"0.008"},{"price":"13487982","size":"0.007"},{"price":"13487905","size":"0.047"},{"price":"13487852","size":"0.02"},{"price":"13487809","size":"0.04"},{"price":"13486823","size":"0.032"},{"price":"13486822","size":"0.111"},{"price":"13486358","size":"0.205"},{"price":"13485688","size":"0.04"},{"price":"13485248","size":"0.01"},{"price":"13484613","size":"0.04"},{"price":"13484207","size":"0.001"},{"price":"13484137","size":"0.03"},{"price":"13483892","size":"0.45"},{"price":"13483764","size":"0.03"},{"price":"13483689","size":"0.005"},{"price":"13483597","size":"0.03"},{"price":"13482759","size":"0.03"},{"price":"13482610","size":"0.001"},{"price":"13482394","size":"0.01"},{"price":"13481838","size":"0.03"},{"price":"13481500","size":"0.4"},{"price":"13481188","size":"0.028"},{"price":"13481029","size":"0.016"}],"symbol":"BTC_JPY","timestamp":"2025-04-30T13:09:45.326Z","grouping":"1"}
            cur_book = self.market_book[ccypair]
            cur_book.timestamp = datetime.datetime.now(datetime.timezone.utc)
            cur_book.market_created_timestamp = datetime.datetime.strptime(
                message["timestamp"][:23].replace("T", " ").replace("Z", ""),
                "%Y-%m-%d %H:%M:%S.%f",
            ).replace(tzinfo=pytz.timezone("UTC"))
            cur_book.bids = SortedDict({float(msg["price"]): float(msg["size"]) for msg in message["bids"]})
            cur_book.asks = SortedDict({float(msg["price"]): float(msg["size"]) for msg in message["asks"]})
            cur_book.universal_id = uuid4().hex
            cur_book.misc = "whole"
            self.market_book[ccypair] = cur_book

            # distribute
            if self.callbacks: asyncio.create_task(asyncio.gather(*[callback(deepcopy(cur_book)) for callback in self.callbacks]))  # send(wo-awaiting)
            if self.last_emit_market_book[ccypair] + datetime.timedelta(milliseconds=250) < cur_book.timestamp:
                asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(cur_book.to_dict())))  # store
                self.last_emit_market_book[ccypair] = cur_book.timestamp

            # create rate message
            new_rate = self.market_book[ccypair].to_rate()
            if self.rate[ccypair].mid_price != new_rate.mid_price:
                self.rate[ccypair] = new_rate
                if self.callbacks: asyncio.create_task(asyncio.gather(*[callback(deepcopy(new_rate)) for callback in self.callbacks]))
                asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(new_rate.to_dict())))  # store

            if self.heatbeat_memo + datetime.timedelta(seconds=60) < cur_book.timestamp:
                self.heatbeat_memo = cur_book.timestamp
                last_update = "\n".join([f"{book.sym}\t{book.timestamp.isoformat()}" for sym, book in self.market_book.items()])
                self.logger.info(f"heartbeat:\n{last_update}")


if __name__ == '__main__':
    client = GmoCryptSocketClient(ccy_pairs=["BTC_JPY"])
    try:
        asyncio.run(client.start())
    except KeyboardInterrupt:
        print("Disconnected")
        asyncio.run(client.close())
