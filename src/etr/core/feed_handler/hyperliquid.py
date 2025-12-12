import asyncio
import websockets
import json
import numpy as np
import datetime
import pytz
import aiohttp
import pandas as pd
from typing import Callable, Awaitable, Optional, List, Dict, Union, Tuple
from sortedcontainers import SortedDict
from uuid import uuid4
from copy import deepcopy
from pathlib import Path

from etr.core.async_logger import AsyncBufferedLogger
from etr.config import Config
from etr.core.datamodel import MarketBook, MarketTrade, Rate, VENUE
from etr.common.logger import LoggerFactory
from etr.core.ws import LocalWsPublisher


class HyperliquidSocketClient:
    def __init__(
        self,
        ccy_pairs: List[str] = ["HYPE", "HYPEUSDC"],
        reconnect_attempts: Optional[int] = None,  # no limit
        publisher: Optional[LocalWsPublisher] = None,
        limit_candle_symbols = False,
    ):
        self.ws_url = "wss://api.hyperliquid.xyz/ws"
        self.rest_url = "https://api.hyperliquid.xyz/info"
        self.publisher = publisher
        self.limit_candle_symbols = limit_candle_symbols

        # logger
        log_file = Path(Config.LOG_DIR).joinpath("main.log").as_posix()
        self.logger = LoggerFactory().get_logger(logger_name="main", log_file=log_file)
        
        # status flags
        self._ws = None
        self._connected = False
        self._running = True
        self.reconnect_attempts = reconnect_attempts
        self.ccy_pairs = ccy_pairs
        self.symbol_table = pd.DataFrame()
        self.symbol_to_pair: Dict[str, str] = {}  # {exchange symbol: ccypair} ... e.g. {@1: HFUNUSDC}

        # instance variables
        self.channels = []
        self.ticker_plant: Dict[str, AsyncBufferedLogger] = {}
        self.market_book: Dict[str, MarketBook] = {}
        self.rate: Dict[str, Rate] = {}
        self.diff_message_buffer: Dict[str, SortedDict] = {}
        self.last_emit_market_book = {}
        self.ohlc_cache = {}

    async def fetch_symbols(self, raw_response=False) -> Union[pd.DataFrame, Tuple]:
        # Get mapping of exchange symbol -> (local) ccypair name
        async def closure(session, request_type: str):
            async with session.post(self.rest_url, json={"type": request_type}, headers={"Content-Type": "application/json"}) as resp:
                return await resp.json()
        
        perp_meta = {}
        spot_meta = {}
        try:
            self.logger.info("Try fetching symbol list")
            async with aiohttp.ClientSession() as session:
                perp_meta = await closure(session, "metaAndAssetCtxs")
                spot_meta = await closure(session, "spotMeta")
        except Exception as e:
            self.logger.info(f"Exception occurred during fetching symbol list: {e}")
        
        if raw_response:
            return (perp_meta, spot_meta)
        elif len(perp_meta) > 0 and len(spot_meta) > 0:
            # join perp + spot(after decoding symbol)
            self.logger.info("Successfully retrieved symbol list")
            univ_perp = pd.DataFrame(perp_meta[0]["universe"])
            univ_spot = pd.DataFrame(spot_meta["universe"])
            tokens_spot = pd.DataFrame(spot_meta["tokens"]).set_index("index")
            univ_spot["pairName"] = tokens_spot.loc[univ_spot.tokens.str[0], "name"].values + tokens_spot.loc[univ_spot.tokens.str[1], "name"].values
            univ_perp["pairName"] = univ_perp["name"]

            pair_list = pd.concat([
                univ_spot[["name", "pairName"]].assign(category="spot"),
                univ_perp[["pairName", "name", "isDelisted"]].assign(category="perp"), 
            ]).fillna(False)
            return pair_list
        else:
            return pd.DataFrame()

    async def start_symbol_refresh_loop(self):
        while self._running:
            data = await self.fetch_symbols()
            if len(data) > 0:
                self.symbol_table = data
                self.symbol_to_pair = self.symbol_table.query("not isDelisted").set_index("name").pairName.to_dict()
                self.logger.info(f"Symbol list refreshed.\n{self.symbol_to_pair}")
            else:
                self.logger.info(f"Failed to refresh ccypair list, retry in 1min")
                await asyncio.sleep(60)
                continue

            now = datetime.datetime.now(datetime.timezone.utc)
            next_time = pd.Timestamp(now).ceil("1D")  # UTC00
            wait_sec = (next_time - now).total_seconds()
            self.logger.info(f"Next refresh scheduled at {next_time.strftime('%Y-%m-%d %H:%M')} (in {wait_sec:.0f} seconds)")
            await asyncio.sleep(wait_sec)

    async def start_ohlc_write_loop(self):
        while self._running:
            # dump current ohlc cache if (is not saved even after close_time)
            now = datetime.datetime.now(datetime.timezone.utc).isoformat()
            for ccypair, ohlc in self.ohlc_cache.items():
                if ohlc["close_time"] < now and not ohlc["_emitted"]:
                    ohlc["_emitted"] = True
                    data = deepcopy(ohlc)
                    data.pop("_emitted")
                    data["timestamp"] = now
                    if self.publisher is not None: asyncio.create_task(self.publisher.send(data))
                    asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(data))) # store
            # wait until next loop
            now = datetime.datetime.now(datetime.timezone.utc)
            next_time = pd.Timestamp(now).ceil("1min")
            wait_sec = (next_time - now).total_seconds()
            await asyncio.sleep(wait_sec)

    async def initialize_attributes(self):

        # retrieve symbol master
        self.symbol_table = await self.fetch_symbols()
        self.symbol_to_pair = self.symbol_table.query("not isDelisted").set_index("name").pairName.to_dict()
        self.pair_to_category = self.symbol_table.set_index("pairName").category.to_dict()

        # set ticker plant (for all available symbols)
        tp_dir = Path(Config.TP_DIR)

        # set channels & instance variables
        self.channels = []
        self.ticker_plant: Dict[str, AsyncBufferedLogger] = {}
        self.market_book: Dict[str, MarketBook] = {}
        self.rate: Dict[str, Rate] = {}
        self.diff_message_buffer: Dict[str, SortedDict] = {}
        self.last_emit_market_book = {}
        self.ohlc_cache = {}
        for symbol, ccy_pair in self.symbol_to_pair.items():
            # Candles
            if not self.limit_candle_symbols or (ccy_pair in self.ccy_pairs):
                self.channels.append({ "type": "candle", "coin": symbol, "interval": "1m"})
            # Markt Trades
            if ccy_pair in self.ccy_pairs:
                self.channels.append({ "type": "trades", "coin": symbol})

            self.market_book[ccy_pair] = MarketBook(sym=ccy_pair.replace("_", "").upper(), venue=VENUE.HYPERLIQUID, category=self.pair_to_category[ccy_pair], misc="null")
            self.rate[ccy_pair] = Rate(sym=ccy_pair.replace("_", "").upper(), venue=VENUE.HYPERLIQUID, category=self.pair_to_category[ccy_pair])
            self.diff_message_buffer[ccy_pair] = SortedDict()
            self.last_emit_market_book[ccy_pair] = datetime.datetime(2000, 1, 1, tzinfo=pytz.timezone("UTC"))
            self.ticker_plant[ccy_pair] = AsyncBufferedLogger(logger_name=f"TP-{self.__class__.__name__}-{ccy_pair.upper().replace('_', '')}", log_dir=tp_dir.as_posix())

    async def start(self):
        asyncio.create_task(self.start_ohlc_write_loop())
        await self.initialize_attributes()

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

    async def _reader_loop(self, websocket):
        while self._connected and self._running:
            raw_msg = await websocket.recv()
            message = json.loads(raw_msg)
            if "channel" not in message:
                self.logger.info(f"{message}")
            else:
                await self._process_message(message)
            self.attempts = 0

    async def _connect(self):
        async with websockets.connect(self.ws_url) as websocket:
            self._connected = True
            self._ws = websocket
            self.logger.info(f"Start subscribing: {self.ws_url}")

            reader_task = asyncio.create_task(self._reader_loop(websocket))
            hb_task = asyncio.create_task(self.heartbeat(websocket))

            # Send request
            for channel in self.channels:
                subscribe_message = {"method": "subscribe", "subscription": channel}
                self.logger.info(f"Send request of {subscribe_message}")
                await websocket.send(json.dumps(subscribe_message))
                await asyncio.sleep(0.1)

            try:
                await reader_task  # ここは適宜調整
            except websockets.exceptions.ConnectionClosedOK:
                self.logger.info("Websoket closed OK")
            except Exception as e:
                self.logger.error(f"Websocket closed ERR: {e}")
                raise
            finally:
                hb_task.cancel()
                reader_task.cancel()
                self._connected = False
                self.logger.info("Close websocket")

    async def close(self):
        self._running = False
        if self._ws:
            await self._ws.close()
        for logger in self.ticker_plant.values():
            logger.stop()

    async def _process_message(self, message: dict):
        
        # folk by message type
        if message.get("channel") == "trades":
            trade_msg = message.get("data")
            for one_trade_dict in trade_msg:
                symbol = one_trade_dict["coin"]
                ccypair = self.symbol_to_pair[symbol]
                data = MarketTrade(
                    timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
                    market_created_timestamp=datetime.datetime.fromtimestamp(one_trade_dict["time"] / 1e3, tz=datetime.timezone.utc),
                    sym=ccypair,
                    venue=VENUE.HYPERLIQUID,
                    category=self.pair_to_category[ccypair],
                    side=1 * (one_trade_dict["side"] == "B") - 1 * (one_trade_dict["side"] == "A"),
                    price=float(one_trade_dict["px"]),
                    amount=float(one_trade_dict["sz"]),
                    trade_id=str(one_trade_dict["tid"]),
                    order_ids=f"{'_'.join(one_trade_dict['users'])}",
                    misc=one_trade_dict["hash"]
                )
                if self.publisher is not None: asyncio.create_task(self.publisher.send(data.to_dict()))
                asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(data.to_dict()))) # store

        elif message.get("channel") == "candle":
            data = message.get("data")
            ccypair = self.symbol_to_pair[data["s"]]
            cur_msg = {
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "open_time": pd.Timestamp(data["t"] * 1e6, tz="UTC").isoformat(),
                "close_time": pd.Timestamp((data["T"] + 1) * 1e6, tz="UTC").isoformat(),
                "sym": ccypair,
                "venue": VENUE.HYPERLIQUID,
                "category": self.pair_to_category[ccypair],
                "open": float(data["o"]),
                "high": float(data["h"]),
                "low": float(data["l"]),
                "close": float(data["c"]),
                "volume": float(data["v"]),
                "counts": int(data["n"]),
                "_data_type": "Candle",
                "_emitted": False,
            }
            prev = self.ohlc_cache.get(ccypair)
            if prev is None:
                self.ohlc_cache[ccypair] = cur_msg
            elif prev["open_time"] < cur_msg["open_time"]:
                if not prev["_emitted"]:
                    # save previous ohlc as close OHLC
                    prev.pop("_emitted")
                    prev["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    if self.publisher is not None: asyncio.create_task(self.publisher.send(prev))
                    asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(prev))) # store
                self.ohlc_cache[ccypair] = cur_msg

        elif message.get("channel") == 'subscriptionResponse':
            self.logger.info(f"{message}")
        else:
            self.logger.info(f"Unknown channel message: {message}")

    async def heartbeat(self, ws, interval=60):
        while True:
            if ws.open:
                self.logger.info(f"(heartbeat) Hyperliquid WebSocket is alive")
            else:
                self.logger.warning(f"(heartbeat) Hyperliquid WebSocket is closed")
                break
            await asyncio.sleep(interval)


if __name__ == '__main__':
    client = HyperliquidSocketClient(ccy_pairs=["BTC"], limit_candle_symbols=True)
    try:
        asyncio.run(client.start())
    except KeyboardInterrupt:
        print("Disconnected")
        asyncio.run(client.close())
