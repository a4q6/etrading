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

from etr.common.logger import LoggerFactory
from etr.core.async_logger import AsyncBufferedLogger
from etr.core.datamodel import MarketTrade, MarketBook, Rate, VENUE
from etr.config import Config
from etr.core.ws import LocalWsPublisher


class BinanceSocketClient:
    """
        Websocket feed handler for Binance.
        https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md
    """
    def __init__(
        self,
        ccy_pairs: List[str] = ["BTCUSDT"],
        reconnect_attempts: Optional[int] = None,  # no limit
        publisher: Optional[LocalWsPublisher] = None,
    ):
        # "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker"
        self.ws_url = 'wss://stream.binance.com:9443/stream?streams='
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
            #  {"BTCUSDT": ['aggTrade', 'bookTicker']}
            # [f"{sym.lower()}@{channel}" for sym, channels in channel_dict.items() for channel in channels])
            self.channels.append(f"{ccy_pair.lower()}@aggTrade")
            self.channels.append(f"{ccy_pair.lower()}@bookTicker")
            self.rate[ccy_pair.upper()] = Rate(sym=ccy_pair.replace("_", "").upper(), venue=VENUE.BINANCE, misc="null")
            # self.last_emit_market_book[ccy_pair] = datetime.datetime(2000, 1, 1, tzinfo=pytz.timezone("UTC"))
            # self.market_book[ccy_pair] = MarketBook(sym=ccy_pair.replace("_", "").upper(), venue=VENUE.BINANCE, misc=["null", 0])

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
        streams = "/".join(self.channels)
        url = self.ws_url + streams
        self.logger.info(f"Connecting to {url}")
        async with websockets.connect(url) as websocket:
            self._connected = True
            self._ws = websocket
            # Send request
            # subscribe_message = json.dumps({"method": "SUBSCRIBE", "params": list(self.channels), "id": 1})
            # await websocket.send(json.dumps(subscribe_message))
            # self.logger.info(f"Send request for '{subscribe_message}'")
            # await asyncio.sleep(0.5)
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

    async def _process_message(self, message: Dict):

        if "stream" not in message.keys():
            pass

        elif "bookTicker" in message["stream"]:
            # bookTicker
            # {
            #     "u":400900217,     // order book updateId
            #     "s":"BNBUSDT",     // symbol
            #     "b":"25.35190000", // best bid price
            #     "B":"31.21000000", // best bid qty
            #     "a":"25.36520000", // best ask price
            #     "A":"40.66000000"  // best ask qty
            # }
            message = message["data"]
            sym = message["s"].upper().replace("_", "")
            best_bid = float(message["b"])
            best_ask = float(message["a"])
            if (best_ask != self.rate[sym].best_ask) or (best_bid != self.rate[sym].best_bid) or (self.rate[sym].misc == "null"):
                self.rate[sym].timestamp = datetime.datetime.now(datetime.timezone.utc)
                self.rate[sym].universal_id = str(message["u"])
                self.rate[sym].best_bid = best_bid
                self.rate[sym].best_ask = best_ask
                self.rate[sym].mid_price = (best_bid + best_ask) / 2
                self.rate[sym].misc = "spot"
                new_rate = deepcopy(self.rate[sym])
                if self.publisher is not None: asyncio.create_task(self.publisher.send(new_rate.to_dict()))
                asyncio.create_task(self.ticker_plant[sym].info(json.dumps(new_rate.to_dict())))  # store

        elif "aggTrade" in message["stream"]:
            # trades
            # {
            #     "e": "aggTrade",  // Event type
            #     "E": 123456789,   // Event time
            #     "s": "BNBBTC",    // Symbol
            #     "a": 12345,       // Aggregate trade ID
            #     "p": "0.001",     // Price
            #     "q": "100",       // Quantity
            #     "f": 100,         // First trade ID
            #     "l": 105,         // Last trade ID
            #     "T": 123456785,   // Trade time
            #     "m": true,        // Is the buyer the market maker?
            #     "M": true         // Ignore
            # }
            message = message["data"]
            sym =  message["s"].upper().replace("_", "")
            data = MarketTrade(
                timestamp = datetime.datetime.now(datetime.timezone.utc),
                market_created_timestamp = datetime.datetime.fromtimestamp(float(message["T"]) / 1e3).replace(tzinfo=pytz.timezone("UTC")),
                sym = message["s"].upper().replace("_", ""),
                venue = VENUE.BINANCE,
                category = "spot",
                side = -1 if message["m"] else +1,
                price = float(message["p"]),
                amount = float(message["q"]),
                trade_id = str(message['a']),
                order_ids = f"{message['f']}_{message['l']}",
                misc = "spot",
            )
            if self.publisher is not None: await self.publisher.send(data.to_dict())
            asyncio.create_task(self.ticker_plant[sym].info(json.dumps(data.to_dict()))) # store


if __name__ == '__main__':
    client = BinanceSocketClient()
    try:
        asyncio.run(client.start())
    except KeyboardInterrupt:
        print("Disconnected")
        asyncio.run(client.close())
