import asyncio
import websockets
import json
import numpy as np
import datetime
import pytz
import itertools
from typing import Callable, Awaitable, Optional, List, Dict
from sortedcontainers import SortedDict
from uuid import uuid4
from copy import deepcopy
from pathlib import Path

from etr.core.async_logger import AsyncBufferedLogger
from etr.config import Config
from etr.core.datamodel import MarketBook, MarketTrade, Rate, VENUE
from etr.common.logger import LoggerFactory


class BitmexSocketClient:
    def __init__(
        self,
        ccy_pairs=["XBTUSD"],
        callbacks: List[Callable[[dict], Awaitable[None]]] = [],
    ):
        self.endpoint = "wss://www.bitmex.com/realtime"
        self.symbols = ccy_pairs
        self.callbacks = callbacks

        # logger
        log_file = Path(Config.LOG_DIR).joinpath("main.log").as_posix()
        tp_file = Path(Config.TP_DIR)
        self.ticker_plant: Dict[str, AsyncBufferedLogger] = {
            ccy_pair: AsyncBufferedLogger(logger_name=f"TP-{self.__class__.__name__}-{ccy_pair}", log_dir=tp_file.as_posix())
            for ccy_pair in ccy_pairs
        }
        self.logger = LoggerFactory().get_logger(logger_name="main", log_file=log_file)

        self.ws = None
        self._running = True
        self.subscriptions = []
        self.market_book: Dict[str, MarketBook] = {}
        self.rate: Dict[str, Rate] = {}
        self.diff_message_buffer: Dict[str, SortedDict] = {}
        self._id2price = {(sym, side): SortedDict() for (sym, side) in itertools.product(ccy_pairs, ["Buy", "Sell"])}
        self.last_emit_market_book = {}
        for ccy_pair in self.symbols:
            self.subscriptions.append(f"trade:{ccy_pair}")
            self.subscriptions.append(f"orderBookL2_25:{ccy_pair}")
            self.market_book[ccy_pair] = MarketBook(sym=ccy_pair.replace("_", "").upper(), venue=VENUE.BITMEX, misc="null")
            self.rate[ccy_pair] = Rate(sym=ccy_pair.replace("_", "").upper(), venue=VENUE.BITMEX)
            self.diff_message_buffer[ccy_pair] = SortedDict()
            self.last_emit_market_book[ccy_pair] = datetime.datetime(2000, 1, 1, tzinfo=pytz.timezone("UTC"))
        

    async def start(self):
        self.attempts = 0
        while self._running:
            try:
                await self.connect()
            except Exception as e:
                self.attempts += 1
                self.logger.error(f"Connection Error (#Attempts={self.attempts}): {e}", exc_info=True)
                sleep_time = 10 * np.log(self.attempts + 0.1)
                await asyncio.sleep(sleep_time)

    async def connect(self):
        async with websockets.connect(self.endpoint) as ws:
            self.logger.info(f"Start subscribing {self.endpoint}")
            self.ws = ws
            try:
                await self.subscribe()
                self._ping_task = asyncio.create_task(self._ping_loop())
                await self.listen()
            except websockets.exceptions.ConnectionClosedOK:
                self.logger.info("Websoket closed OK")
            except Exception as e:
                self.logger.error(f"Websocket closed ERR: {e}")
                raise
            finally:
                self.logger.info("Close websocket")
                self._ping_task.cancel()

    async def subscribe(self):
        for sub in self.subscriptions:
            self.logger.info(f"Send request for {sub}")
            payload = {"op": "subscribe", "args": [sub]}
            await self.ws.send(json.dumps(payload))

    async def listen(self):
        async for message in self.ws:
            if message == "pong":
                self.logger.info(f"Received {message}")
            else:
                data = json.loads(message)
                await self.on_message(data)
                self.attempts = 0

    async def _ping_loop(self):
        while self._running:
            try:
                if self.ws:
                    await self.ws.send("ping")
                    self.logger.info("Sent ping")
                await asyncio.sleep(60)
            except Exception as e:
                self.logger.error(f"Ping loop error: {e}")
                await asyncio.sleep(5)

    async def close(self):
        self._running = False
        if self.ws:
            await self.ws.close()
            self._ping_task.cancel()
        for logger in self.ticker_plant.values():
            logger.stop()

    async def on_message(self, message):

        if message.get("table") == "trade":
            for one_trade_dict in message.get("data"):
                sym = one_trade_dict["symbol"]
                data = MarketTrade(
                    timestamp = datetime.datetime.now(datetime.timezone.utc),
                    market_created_timestamp = datetime.datetime.strptime(one_trade_dict["timestamp"][:23].replace("T"," ").replace("Z", ""), "%Y-%m-%d %H:%M:%S.%f"),
                    sym = sym,
                    venue = VENUE.BITMEX,
                    category = None,
                    side = 1 * (one_trade_dict["side"] == 'Buy') - 1 * (one_trade_dict["side"] == 'Sell'),
                    price = one_trade_dict["price"],
                    amount = one_trade_dict["homeNotional"],
                    trade_id = str(one_trade_dict["trdMatchID"]),
                    order_ids = "",
                    universal_id = uuid4().hex,
                    misc = one_trade_dict["tickDirection"],
                )
                if self.callbacks: asyncio.create_task(asyncio.gather(*[callback(data) for callback in self.callbacks]))  # send(wo-awaiting)
                asyncio.create_task(self.ticker_plant[sym].info(json.dumps(data.to_dict()))) # store

        elif message.get("table") == "orderBookL2_25":
            # extract unique syms
            syms = set([])
            for data in message["data"]:
                syms.add(data["symbol"])

            last_update_timestamp = {sym: self.market_book[sym].timestamp for sym in syms}
            
            # write book data into local
            if message['action'] == 'partial' or message['action'] == 'insert':
                if message['action'] == 'partial':
                    # reset book if partial
                    for sym in syms:
                        self.market_book[sym].bids = SortedDict()
                        self.market_book[sym].asks = SortedDict()
                        self._id2price[(sym, "Buy")] = SortedDict()
                        self._id2price[(sym, "Sell")] = SortedDict()
                
                for data in message["data"]:
                    # write book
                    sym = data["symbol"]
                    if message["action"] == "partial" or self.market_book[sym].misc != "null":
                        self._id2price[(sym, data["side"])][data["id"]] = data["price"]
                        if data["side"] == 'Buy':
                            self.market_book[sym].bids[data["price"]] = data["size"]
                        elif data["side"]  == 'Sell':
                            self.market_book[sym].asks[data["price"]] = data["size"]
                    
            elif message['action'] == 'update':
                for data in message["data"]:
                    # update book
                    sym = data["symbol"]
                    if self.market_book[sym].misc != "null":
                        if data["side"] == 'Buy':
                            self.market_book[sym].bids[ self._id2price[(sym, 'Buy')][data["id"]] ] = data["size"]
                        elif data["side"]  == 'Sell':
                            self.market_book[sym].asks[ self._id2price[(sym, 'Sell')][data["id"]] ] = data["size"]
                        
            elif message['action'] == 'delete':
                for data in message["data"]:
                    # delete id and book
                    sym = data["symbol"]
                    if self.market_book[sym].misc != "null":
                        if data["id"] in self._id2price[(sym, data["side"])].keys():
                            # drop price book 
                            if data["side"] == 'Buy':
                                self.market_book[sym].bids.pop( self._id2price[(sym,  'Buy')][data["id"]] )
                            elif data["side"]  == 'Sell':
                                self.market_book[sym].asks.pop( self._id2price[(sym, 'Sell')][data["id"]] )
                            # drop corresponding id
                            self._id2price[(sym, data["side"])].pop(data["id"])
                
            # update other attributes
            for sym in syms:
                self.market_book[sym].timestamp = datetime.datetime.now(datetime.timezone.utc)
                self.market_book[sym].universal_id = uuid4().hex
                self.market_book[sym].misc = message["action"]

            # distribute
            for sym in syms:
                cur_book = deepcopy(self.market_book[sym])
                if self.callbacks: asyncio.create_task(asyncio.gather(*[callback(deepcopy(cur_book)) for callback in self.callbacks]))  # send(wo-awaiting)
                if self.last_emit_market_book[sym] + datetime.timedelta(milliseconds=250) < cur_book.timestamp:
                    asyncio.create_task(self.ticker_plant[sym].info(json.dumps(cur_book.to_dict())))  # store
                    self.last_emit_market_book[sym] = cur_book.timestamp

        if message.get("table") == "orderBookL2_25":
            # Create Rate message from local market book
            for sym, book in self.market_book.items():
                if book.misc != "null":
                    new_rate = book.to_rate()
                    if self.rate[sym].mid_price != new_rate.mid_price:
                        self.rate[sym] = new_rate
                        if self.callbacks: asyncio.create_task(asyncio.gather(*[callback(deepcopy(new_rate)) for callback in self.callbacks]))
                        asyncio.create_task(self.ticker_plant[sym].info(json.dumps(new_rate.to_dict())))  # store


if __name__ == "__main__":
    client = BitmexSocketClient()
    try:
        asyncio.run(client.start())
    except KeyboardInterrupt:
        print("Disconnected")
        asyncio.run(client.close())
