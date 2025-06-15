import asyncio
import datetime
import json
import pandas as pd
import time
import hmac
import hashlib
import aiohttp
import json
import pytz
from uuid import uuid4
from typing import Optional, Dict, List
from pathlib import Path
from pubnub.pubnub import PubNub
from pubnub.pnconfiguration import PNConfiguration
from pubnub.callbacks import SubscribeCallback

from etr.core.async_logger import AsyncBufferedLogger
from etr.core.datamodel import VENUE, Order, Trade, OrderStatus, OrderType
from etr.core.api_client.base import ExchangeClientBase
from etr.core.ws import LocalWsPublisher
from etr.strategy.base_strategy import StrategyBase
from etr.config import Config
from etr.common.logger import LoggerFactory


class BitbankPrivateStreamClient(SubscribeCallback):
    """
    https://github.com/bitbankinc/bitbank-api-docs/blob/master/private-stream_JP.md
    """

    SUBKEY = "sub-c-ecebae8e-dd60-11e6-b6b1-02ee2ddab7fe"

    def __init__(
        self,
        publisher: Optional[LocalWsPublisher] = None,
        time_window = "5000",
        api_key: str = Config.BITBANK_API_KEY,
        api_secret: str = Config.BITBANK_API_SECRET,
    ):
        # auth & endpoint
        self.ws_url = "wss://stream.bitbank.cc/socket.io/?EIO=4&transport=websocket"
        self.api_key = api_key
        self.api_secret = api_secret.encode()
        self.time_window = time_window

        # logger
        log_file = Path(Config.LOG_DIR).joinpath("main.log").as_posix()
        tp_file = Path(Config.TP_DIR)
        self.ticker_plant = AsyncBufferedLogger(logger_name=f"TP-BitBankPrivate-ALL", log_dir=tp_file.as_posix())
        self.logger = LoggerFactory().get_logger(logger_name="main", log_file=log_file)
        self.publisher = publisher
        self.event_queue: asyncio.Queue = asyncio.Queue()


    async def get_token_and_channel(self) -> List[str]:
        # Get auth token
        nonce = str(int(time.time() * 1000))
        path = '/v1/user/subscribe'
        message = f"{nonce}{self.time_window}{path}"
        signature = hmac.new(
            self.api_secret,
            message.encode(),
            hashlib.sha256
        ).hexdigest()

        headers = {
            "Content-Type": "application/json",
            "ACCESS-KEY": self.api_key,
            "ACCESS-REQUEST-TIME": nonce,
            "ACCESS-TIME-WINDOW": self.time_window,
            "ACCESS-SIGNATURE": signature,
        }

        url = "https://api.bitbank.cc" + path
        self.logger.info(f"Request new token to {url}")
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                res = await response.json()
                self.token_info = res["data"]
                return self.token_info["pubnub_channel"], self.token_info["pubnub_token"]

    async def connect(self):
        self.channel, self.token = await self.get_token_and_channel()

        config = PNConfiguration()
        config.subscribe_key = "sub-c-ecebae8e-dd60-11e6-b6b1-02ee2ddab7fe"
        config.uuid = self.channel
        config.auth_key = self.token  # 一部SDKでは auth_key or setToken を利用

        self.logger.info("Start subscribing bitbank private stream")
        self.pubnub = PubNub(config)
        self.pubnub.add_listener(self)
        self.pubnub.set_token(self.token)
        self.pubnub.subscribe().channels(self.channel).execute()

    def disconnect(self):
        self.pubnub.unsubscribe().channels(self.channel).execute()
        self.pubnub.stop()

    def status(self, pubnub, status):
        self.logger.info(f"[STATUS] Category: {status.category}")
        if status.category == "PNAccessDeniedCategory":
            self.logger.info("Token expired, reconnecting...")
            asyncio.create_task(self.reconnect())

    async def reconnect(self):
        self.pubnub.unsubscribe().channels([self.channel]).execute()
        await self.connect()

    def message(self, pubnub, message):
        """PubNubからのメッセージ受信時に呼ばれる（非同期キューへ）"""
        data = json.dumps(message.message)
        asyncio.create_task(self.event_queue.put(data))

    async def _process_events(self):
        """PubNubイベント処理の非同期ループ"""
        while True:
            data = await self.event_queue.get()
            event_type = data.get("method")
            event_msg = data.get("params")

            if event_type == "asset_update":
                pass
            elif event_type == "spot_order_new":
                await self._handle_order_message(event_msg, event_type)
            elif event_type == "spot_order":
                await self._handle_order_message(event_msg, event_type)
            elif event_type == "spot_trade":
                await self._handle_trade_message(event_msg)
            elif event_type == "dealer_order_new":
                pass
            elif event_type == "margin_position_update":
                await self._handle_position_update(event_msg)
            else:
                pass

    async def _handle_order_message(self, msg: List[Dict], event_type: str):
        for o in msg:
            status = self._convert_order_status(msg["status"])
            if status == OrderStatus.Canceled:
                mkt_time = datetime.datetime.fromtimestamp(msg["canceled_at"] / 1000).replace(tzinfo=pytz.timezone("UTC"))
            elif status in (OrderStatus.Partial, OrderStatus.Filled):
                mkt_time = datetime.datetime.fromtimestamp(msg["executed_at"] / 1000).replace(tzinfo=pytz.timezone("UTC"))
            elif msg["type"] == "stop" and msg["is_just_triggered"]:
                mkt_time = datetime.datetime.fromtimestamp(msg["triggered_at"] / 1000).replace(tzinfo=pytz.timezone("UTC"))
            else:
                mkt_time = datetime.datetime.fromtimestamp(msg["ordered_at"] / 1000).replace(tzinfo=pytz.timezone("UTC"))
            if msg["type"] in ("limit"):
                price = msg["price"]
            elif msg["type"] in ("market") or (msg["type"] == "stop" and status == OrderStatus.Filled):
                price = msg["average_price"]
            elif msg["type"] in ("stop"):
                price = msg["trigger_price"]

            order = Order(
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                market_created_timestamp=mkt_time,
                sym=msg["pair"].replace("_", "").upper(),
                side=1 if msg["side"] == "buy" else -1,
                price=float(price),
                amount=float(msg["executed_amount"]) + float(msg["remaining_amount"]),
                executed_amount=float(msg["executed_amount"]),
                order_type=msg["type"],
                order_status=status,
                venue=VENUE.BITBANK,
                order_id=str(msg["order_id"]),
                model_id=None,
                process_id=None,
                src_type=None,
                src_id = None,
                src_timestamp=pd.NaT,
                misc=f"streaming-{event_type}"
            )
            if self.publisher is not None: asyncio.create_task(self.publisher.send(order.to_dict()))
            asyncio.create_task(self.ticker_plant.info(json.dumps(order.to_dict())))

    async def _handle_trade_message(self, msg: List[Dict]):
        for trade_msg in msg:
            sym = trade_msg["pair"].replace("_", "").upper()  # "btc_jpy" → "BTCJPY"
            misc = {
                "pnl": float(trade_msg["profit_loss"]), 
                "interest": float(trade_msg["interest"]),
                "MT": trade_msg["maker_taker"],
                "channel": "streaming",
            }
            trade = Trade(
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                market_created_timestamp=datetime.datetime.fromtimestamp(
                    trade_msg["timestamp"] / 1000, tz=datetime.timezone.utc
                ),
                sym=sym,
                venue=VENUE.BITBANK,
                side=1 if trade_msg["side"] == "buy" else -1,
                price=float(trade_msg["price"]),
                amount=float(trade_msg["amount"]),
                order_id=str(trade_msg["order_id"]),
                order_type=trade_msg["type"],  # "limit" or "market" or "stop"
                trade_id=str(trade_msg["exec_id"]),
                misc=str(misc),
            )
            if self.publisher is not None: asyncio.create_task(self.publisher.send(trade.to_dict()))
            asyncio.create_task(self.ticker_plant.info(json.dumps(trade.to_dict())))

    async def _handle_position_update(self, msg: List[Dict]):
        for pos in msg:
            data = {
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "sym": pos["pair"].replace("_", "").upper(),
                "venue": VENUE.BITBANK,
                "side": +1 if pos["side"] == "long" else -1,
                "vwap": float(pos["average_price"]),
                "amount": float(pos["open_amount"]),
                "locked_amount": float(pos["locked_amount"]),
                "unrealized_fee_amount": float(pos["unrealized_fee_amount"]),
                "unrealized_interest_amount": float(pos["unrealized_interest_amount"]),
                "universal_id": uuid4().hex,
                "_data_type": "PositionUpdate",
            }
            if self.publisher is not None: asyncio.create_task(self.publisher.send(data))
            asyncio.create_task(self.ticker_plant.info(json.dumps(data)))

    def _convert_order_status(self, status: str) -> OrderStatus:
        mapping = {
            "INACTIVE": OrderStatus.Sent,
            "UNFILLED": OrderStatus.Sent,
            "PARTIALLY_FILLED": OrderStatus.Partial, 
            "FULLY_FILLED": OrderStatus.Filled,
            "CANCELED_UNFILLED": OrderStatus.Canceled,
            "CANCELED_PARTIALLY_FILLED": OrderStatus.Canceled,
        }
        return mapping.get(status.upper(), None)
