import asyncio
import websockets
import json
import numpy as np
import datetime
import pytz
import aiohttp
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


class BitFlyerSocketClient:
    def __init__(
        self,
        ccy_pairs: List[str] = ["BTC_JPY", "FX_BTC_JPY"],
        callbacks: List[Callable[[dict], Awaitable[None]]] = [],
        reconnect_attempts: Optional[int] = None,  # no limit
    ):
        self.ws_url = "wss://ws.lightstream.bitflyer.com/json-rpc"
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
        self.last_emit_market_book = {}
        for ccy_pair in ccy_pairs:
            self.channels.append(f"lightning_board_snapshot_{ccy_pair}")
            self.channels.append(f"lightning_executions_{ccy_pair}")
            self.channels.append(f"lightning_board_{ccy_pair}")
            self.market_book[ccy_pair] = MarketBook(sym=ccy_pair.replace("_", "").upper(), venue=VENUE.BITFLYER, category="json-rpc", misc="null")
            self.rate[ccy_pair] = Rate(sym=ccy_pair.replace("_", "").upper(), venue=VENUE.BITFLYER, category="json-rpc")
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
                subscribe_message = {"method": "subscribe", "params": {"channel": channel}}
                await websocket.send(json.dumps(subscribe_message))
                self.logger.info(f"Send request for '{channel}'")

            try:
                asyncio.create_task(self.heartbeat(websocket))
                while self._connected and self._running:
                    raw_msg = await websocket.recv()
                    message = json.loads(raw_msg)
                    if not "params" in message.keys():
                        self.logger.info(f"no `params` key in message. pass this message.")
                        self.logger.info(f"{message}")
                    else:
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
        
        # folk by message type
        if "params" not in message.keys() or "channel" not in message["params"].keys():
            self.logger.info(f"Unknown message type: {message}")
            return

        elif "executions" in message["params"]["channel"]:
            ccypair = message["params"]["channel"].split("executions_")[-1]
            for one_trade_dict in message["params"]["message"]:
                data = MarketTrade(
                    timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
                    market_created_timestamp=datetime.datetime.strptime(
                        one_trade_dict["exec_date"][:23].replace("T", " ").replace("Z", ""),
                        "%Y-%m-%d %H:%M:%S.%f",
                    ).replace(tzinfo=pytz.timezone("UTC")),
                    sym=ccypair.replace("_", ""),
                    venue=VENUE.BITFLYER,
                    category="json-rpc",
                    side=1 * (one_trade_dict["side"] == "BUY") - 1 * (one_trade_dict["side"] == "SELL"),
                    price=one_trade_dict["price"],
                    amount=one_trade_dict["size"],
                    trade_id=str(one_trade_dict["id"]),
                    order_ids=f"{one_trade_dict['buy_child_order_acceptance_id']}_{one_trade_dict['sell_child_order_acceptance_id']}",
                )
                if self.callbacks: asyncio.create_task(asyncio.gather(*[callback(data) for callback in self.callbacks]))  # send(wo-awaiting)
                asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(data.to_dict()))) # store

        elif "board_snapshot" in message["params"]["channel"]:
            # renew market book
            ccypair = message["params"]["channel"].split("board_snapshot_")[-1]
            body = message["params"]["message"]  # discard "channel" info
            cur_book = deepcopy(self.market_book[ccypair])
            cur_book.timestamp = datetime.datetime.now(datetime.timezone.utc)
            cur_book.bids = SortedDict({float(msg["price"]): float(msg["size"]) for msg in body["bids"]})
            cur_book.asks = SortedDict({float(msg["price"]): float(msg["size"]) for msg in body["asks"]})
            cur_book.universal_id = uuid4().hex
            cur_book.misc = "whole"
            self.market_book[ccypair] = cur_book
            
            # distribute
            if self.callbacks: asyncio.create_task(asyncio.gather(*[callback(deepcopy(cur_book)) for callback in self.callbacks]))  # send(wo-awaiting)
            asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(cur_book.to_dict())))  # store

        elif "board" in message["params"]["channel"]:
            # update market book
            ccypair = message["params"]["channel"].split("board_")[-1]
            if self.market_book[ccypair].misc != "null":
                cur_book = self.market_book[ccypair]
                cur_book.timestamp = datetime.datetime.now(datetime.timezone.utc)
                cur_book.universal_id = uuid4().hex
                cur_book.misc = "diff"
                body = message["params"]["message"]  # discard "channel" info
                # update market book
                for diff in body["asks"]:
                    if diff["size"] == 0:
                        if diff["price"] in cur_book.asks.keys():
                            cur_book.asks.pop(diff["price"])
                    else:
                        cur_book.asks[diff["price"]] = diff["size"]

                for diff in body["bids"]:
                    if diff["size"] == 0:
                        if diff["price"] in cur_book.bids.keys():
                            cur_book.bids.pop(diff["price"])
                    else:
                        cur_book.bids[diff["price"]] = diff["size"]
                self.market_book[ccypair] = cur_book

                # distribute
                if self.callbacks: asyncio.create_task(asyncio.gather(*[callback(deepcopy(cur_book)) for callback in self.callbacks]))  # send(wo-awaiting)
                if self.last_emit_market_book[ccypair] + datetime.timedelta(milliseconds=250) < cur_book.timestamp:
                    asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(cur_book.to_dict())))  # store
                    self.last_emit_market_book[ccypair] = cur_book.timestamp


        if "board" in message["params"]["channel"]:
            # create Rate message
            if self.market_book[ccypair].misc != "null":
                new_rate = self.market_book[ccypair].to_rate()
                if self.rate[ccypair].mid_price != new_rate.mid_price:
                    self.rate[ccypair] = new_rate
                    if self.callbacks: asyncio.create_task(asyncio.gather(*[callback(deepcopy(new_rate)) for callback in self.callbacks]))
                    asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(new_rate.to_dict())))  # store

    async def heartbeat(self, ws, interval=60):
        while True:
            if ws.open:
                self.logger.info(f"(heartbeat) BitFlyer WebSocket is alive")
            else:
                self.logger.warning(f"(heartbeat) BitFlyer WebSocket is closed")
                break
            await asyncio.sleep(interval)

class BitFlyerFundingRate:

    ENDPOINT = "https://api.bitflyer.com/v1/"

    def __init__(self, ccy_pairs = ["FX_BTC_JPY"]):
        self.ccy_pairs = ccy_pairs
        # logger
        log_file = Path(Config.LOG_DIR).joinpath("main.log").as_posix()
        tp_file = Path(Config.TP_DIR)
        self.ticker_plant: Dict[str, AsyncBufferedLogger] = {
            ccy_pair: AsyncBufferedLogger(
                logger_name=f"TP-{self.__class__.__name__}-{ccy_pair.upper().replace('_', '')}", 
                log_dir=tp_file.as_posix())
            for ccy_pair in ccy_pairs
        }
        self.logger = LoggerFactory().get_logger(logger_name="main", log_file=log_file)

        self.latest_data = {ccy_pair: {} for ccy_pair in ccy_pairs}


    @staticmethod
    def get_next_update_time(now: datetime.datetime) -> datetime.datetime:
        JST = datetime.timezone(datetime.timedelta(hours=9))
        now_jst = now.astimezone(JST)
        update_hours = [6, 14, 22]
        for hour in update_hours:
            next_time = now_jst.replace(hour=hour, minute=0, second=0, microsecond=0)
            if now_jst < next_time:
                return next_time.astimezone(datetime.timezone.utc)
        # next 6am
        next_time = now_jst + datetime.timedelta(days=1)
        next_time = next_time.replace(hour=6, minute=0, second=0, microsecond=0)
        return next_time.astimezone(datetime.timezone.utc)


    async def get_funding_rate(self, ccy_pair: str) -> Dict:
        data = {}
        async with aiohttp.ClientSession() as session:
            params = {"product_code": ccy_pair}
            self.logger.info(f"Send request funding rate of '{ccy_pair}'")
            try:
                async with session.get(self.ENDPOINT + "getfundingrate", params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        data['next_funding_rate_settledate'] = pd.Timestamp(data['next_funding_rate_settledate'], tz="UTC").to_pydatetime().isoformat()
                        data["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                        data["sym"] = ccy_pair.replace("_", "")
                        data["venue"] = VENUE.BITFLYER
                        data["category"] = "REST"
                        data["_data_type"] = "FundingRate"
                    else:
                        self.logger.info(f"Error: HTTP {response.status}")
            except Exception as e:
                self.logger.error(f"Exception occurred while fetching funding rate: {e}", exc_info=True)
        return data

    async def start(self):
        while True:
            # try 1min loop until all the funding rate updated
            updated_flag = {ccypair: False for ccypair in self.ccy_pairs}
            while True:
                for ccypair in self.ccy_pairs:
                    if not updated_flag[ccypair]:
                        data = await self.get_funding_rate(ccypair)
                        if self.latest_data[ccypair].get("next_funding_rate_settledate") != data['next_funding_rate_settledate']:
                            updated_flag[ccypair] = True
                            self.latest_data[ccypair] = data
                            asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(data)))  # store

                if all(updated_flag.values()):
                    break
                else:
                    self.logger.info(f"Failed to retrieve new data, wait 1min to retry requesting : {updated_flag}")
                    await asyncio.sleep(60)

            now = datetime.datetime.now(datetime.timezone.utc)
            next_time = self.get_next_update_time(now)
            wait_sec = (next_time - now).total_seconds() + 5
            self.logger.info(f"Next check scheduled at {next_time.strftime('%Y-%m-%d %H:%M')} (in {wait_sec:.0f} seconds)")
            await asyncio.sleep(wait_sec)


if __name__ == '__main__':
    # client = BitFlyerSocketClient()
    client = BitFlyerFundingRate()
    try:
        asyncio.run(client.start())
    except KeyboardInterrupt:
        print("Disconnected")
        asyncio.run(client.close())
