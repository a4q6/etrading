
import asyncio
import websockets
import json
import numpy as np
import datetime
import pytz
from typing import Callable, Awaitable, Optional, List, Dict
from sortedcontainers import SortedDict
from uuid import uuid4
from copy import deepcopy
from pathlib import Path

from etr.core.async_logger import AsyncBufferedLogger
from etr.config import Config
from etr.core.datamodel import MarketBook, MarketTrade, Rate, VENUE
from etr.common.logger import LoggerFactory
from etr.core.ws import LocalWsPublisher


class CoincheckSocketClient:
    def __init__(
        self,
        ccy_pairs: List[str] = ["btc_jpy"],
        reconnect_attempts: Optional[int] = None,  # no limit
        publisher: Optional[LocalWsPublisher] = None,
    ):
        self.ws_url = "wss://ws-api.coincheck.com/"
        self.publisher = publisher

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
        self.last_emit_market_book = {}
        for ccy_pair in ccy_pairs:
            self.channels.append(f"{ccy_pair}-orderbook")
            self.channels.append(f"{ccy_pair}-trades")
            self.market_book[ccy_pair] = MarketBook(sym=ccy_pair.replace("_", "").upper(), venue=VENUE.COINCHECK, category="websocket", misc="null")
            self.rate[ccy_pair] = Rate(sym=ccy_pair.replace("_", "").upper(), venue=VENUE.COINCHECK, category="websocket")
            self.diff_message_buffer[ccy_pair] = SortedDict()
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
            for channel in self.channels:
                subscribe_message = {'type': 'subscribe', 'channel': channel}
                await websocket.send(json.dumps(subscribe_message))
                self.logger.info(f"Send request for '{channel}'")

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
        
        # market book
        if len(message) == 2 and isinstance(message[1], dict) and "last_update_at" in message[1].keys():
            ccypair = message[0]
            timestamp = pytz.timezone("UTC").localize(datetime.datetime.fromtimestamp(float(message[1]["last_update_at"])))
            # diff msg
            cur_book = self.market_book[ccypair]
            cur_book.timestamp = datetime.datetime.now(datetime.timezone.utc)
            cur_book.market_created_timestamp = timestamp
            cur_book.universal_id = uuid4().hex

            # update market book
            bids = {float(msg[0]): float(msg[1]) for msg in message[1]["bids"]}
            asks = {float(msg[0]): float(msg[1]) for msg in message[1]["asks"]}
            for p, v in asks.items():
                if v == 0:
                    if p in cur_book.asks.keys():
                        cur_book.asks.pop(p)
                else:
                    cur_book.asks[p] = v
            for p, v in bids.items():
                if v == 0:
                    if p in cur_book.bids.keys():
                        cur_book.bids.pop(p)
                else:
                    cur_book.bids[p] = v
            self.market_book[ccypair] = cur_book

            # distribute
            if self.market_book[ccypair].misc != "null":
                if self.publisher is not None: asyncio.create_task(self.publisher.send(cur_book.to_dict()))
                if self.last_emit_market_book[ccypair] + datetime.timedelta(milliseconds=250) < cur_book.timestamp:
                    asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(cur_book.to_dict())))  # store
                    self.last_emit_market_book[ccypair] = cur_book.timestamp

            # rate message
            if self.market_book[ccypair].misc != "null":
                new_rate = self.market_book[ccypair].to_rate()
                if self.rate[ccypair].mid_price != new_rate.mid_price:
                    self.rate[ccypair] = new_rate
                    if self.publisher is not None: asyncio.create_task(self.publisher.send(new_rate.to_dict()))
                    asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(new_rate.to_dict())))  # store

        # market trade
        elif isinstance(message, list) and len(message[0]) == 8:
            for msg in message:
                ccypair = msg[2]
                side = 1 * (msg[-3] == "buy") - 1 * (msg[-3] == "sell")
                price = float(msg[3])
                timestamp = pytz.timezone("UTC").localize(datetime.datetime.fromtimestamp(int(msg[0])))
                data = MarketTrade(
                    timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
                    market_created_timestamp=timestamp,
                    sym=msg[2].replace("_", "").upper(),
                    venue=VENUE.COINCHECK,
                    category="websocket",
                    side=side,
                    price=price,
                    amount=float(msg[4]),
                    trade_id=str(msg[1]),
                    order_ids=f"{msg[-2]}_{msg[-1]}",
                )
                if self.publisher is not None: asyncio.create_task(self.publisher.send(data.to_dict()))
                asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(data.to_dict()))) # store

                # update market book
                if self.market_book[ccypair].market_created_timestamp <= timestamp:
                    if side > 0:
                        self.market_book[ccypair].asks = SortedDict({p: v for p, v in self.market_book[ccypair].asks.items() if price <= p})
                    elif side < 0:
                        self.market_book[ccypair].bids = SortedDict({p: v for p, v in self.market_book[ccypair].bids.items() if p <= price})
                    self.market_book[ccypair].misc = ""

if __name__ == '__main__':
    client = CoincheckSocketClient()
    try:
        asyncio.run(client.start())
    except KeyboardInterrupt:
        print("Disconnected")
        asyncio.run(client.close())
