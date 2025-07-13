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
import logging
import janus
import numpy as np
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
        tp_number: int = 0,
    ):
        # auth & endpoint
        self.ws_url = "wss://stream.bitbank.cc/socket.io/?EIO=4&transport=websocket"
        self.api_key = api_key
        self.api_secret = api_secret.encode()
        self.time_window = time_window
        self._running = False
        self.loop = asyncio.get_event_loop()
        self._last_reconnect_time = 0 

        # logger
        log_file = Path(Config.LOG_DIR).joinpath("main.log").as_posix()
        tp_file = Path(Config.TP_DIR)
        self.ticker_plant = AsyncBufferedLogger(logger_name=f"TP-BitBankPrivate{tp_number}-ALL", log_dir=tp_file.as_posix())
        self.logger = LoggerFactory().get_logger(logger_name="main", log_file=log_file)
        self.publisher = publisher
        self.event_queue = janus.Queue()


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
            
    # async def keep_token_alive(self, interval_sec: int = 300):
    #     """トークン期限切れを防ぐために定期再接続"""
    #     while self._running:
    #         await asyncio.sleep(interval_sec)
    #         self.logger.info("[Token Refresh] Proactively reconnecting...")
    #         try:
    #             self.pubnub.unsubscribe().channels([self.channel]).execute()
    #             await asyncio.sleep(1)
    #         except Exception as e:
    #             self.logger.warning(f"[Token Refresh] Unsubscribe failed: {e}")
    #         await self.connect()

    async def start(self):
        """接続・イベント処理・トークン更新を一括開始"""
        self._running = True
        asyncio.create_task(self._process_events())
        asyncio.create_task(self.connect())
        # asyncio.create_task(self.keep_token_alive(interval_sec=300))  # 5分ごとに再接続

    async def close(self):
        self._running = False
        self.pubnub.unsubscribe().channels([self.channel]).execute()

    async def connect(self):
        self.channel, self.token = await self.get_token_and_channel()

        config = PNConfiguration()
        config.subscribe_key = "sub-c-ecebae8e-dd60-11e6-b6b1-02ee2ddab7fe"
        config.heartbeat_interval = 60
        config.set_presence_timeout(180)
        config.uuid = self.channel
        config.auth_key = self.token  # 一部SDKでは auth_key or setToken を利用

        self.logger.info("Start subscribing bitbank private stream")
        new_pubnub = PubNub(config)
        new_pubnub.add_listener(self)
        new_pubnub.set_token(self.token)
        new_pubnub.subscribe().channels(self.channel).execute()
        self.pubnub = new_pubnub

    def disconnect(self):
        self.pubnub.unsubscribe().channels(self.channel).execute()
        self.pubnub.stop()

    def status(self, pubnub, status):
        self.logger.info(f"[STATUS] Category: {status.category}")
        if time.time() - self._last_reconnect_time > 10 and status.category.name in ("PNAccessDeniedCategory", 'PNTimeoutCategory', 'PNNetworkIssuesCategory'):
            self.logger.info("Token expired or invalid. Attempting reconnect...")
            asyncio.run_coroutine_threadsafe(self.reconnect(), self.loop)

    async def reconnect(self):
        self.logger.info("Reconnecting PubNub...")
        if hasattr(self, "pubnub"):
            try:
                self.pubnub.unsubscribe().channels([self.channel]).execute()
                self.pubnub.stop()  # 安全に完全停止
                await asyncio.sleep(1)  # 明示的なクールダウン
            except Exception as e:
                self.logger.warning(f"Exception during unsubscribe: {e}", exc_info=True)

        try:
            await self.connect()
            self.logger.info("Reconnection complete")
            self._last_reconnect_time = time.time()
        except Exception as e:
            self.logger.error(f"Exception during reconnect(): {e}", exc_info=True)

    def message(self, pubnub, message):
        """PubNubからのメッセージ受信時に呼ばれる（非同期キューへ）"""
        data = message.message
        self.event_queue.sync_q.put(data)
        self.logger.debug(f"{data}")

    async def _process_events(self):
        """PubNubイベント処理の非同期ループ"""
        while self._running:
            try:
                data = await self.event_queue.async_q.get()
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
                    self.logger.warning(f"{event_type} | {event_msg}")
            except KeyboardInterrupt:
                raise KeyboardInterrupt()
            except Exception as e:
                self.logger.error(f"Error while loading data -- {e}", exc_info=True)

    async def _handle_order_message(self, msg: List[Dict], event_type: str):
        for o in msg:
            status = self._convert_order_status(o["status"])
            if status == OrderStatus.Canceled:
                mkt_time = datetime.datetime.fromtimestamp(o["canceled_at"] / 1000)
            elif status in (OrderStatus.Partial, OrderStatus.Filled):
                mkt_time = datetime.datetime.fromtimestamp(o["executed_at"] / 1000)
            elif o["type"] == "stop" and o["is_just_triggered"]:
                mkt_time = datetime.datetime.fromtimestamp(o["triggered_at"] / 1000)
            else:
                mkt_time = datetime.datetime.fromtimestamp(o["ordered_at"] / 1000)
            mkt_time = pytz.timezone("UTC").localize(mkt_time)

            if o["type"] in ("limit"):
                price = o["price"]
            elif o["type"] in ("market") or (o["type"] == "stop" and status == OrderStatus.Filled):
                price = o["average_price"]
            elif o["type"] in ("stop"):
                price = o["trigger_price"]

            order = Order(
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                market_created_timestamp=mkt_time,
                sym=o["pair"].replace("_", "").upper(),
                side=1 if o["side"] == "buy" else -1,
                price=float(price),
                amount=float(o["executed_amount"]) + float(o["remaining_amount"]),
                executed_amount=float(o["executed_amount"]),
                order_type=o["type"],
                order_status=status,
                venue=VENUE.BITBANK,
                order_id=str(o["order_id"]),
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
                "pnl": float(trade_msg["profit_loss"]) if trade_msg["profit_loss"] is not None else np.nan,
                "interest": float(trade_msg["interest"]) if trade_msg["interest"] is not None else np.nan,
                "MT": trade_msg["maker_taker"],
                "channel": "streaming-spot_trade",
            }
            trade = Trade(
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                market_created_timestamp=datetime.datetime.fromtimestamp(
                    trade_msg["executed_at"] / 1000, tz=datetime.timezone.utc
                ),
                sym=sym,
                venue=VENUE.BITBANK,
                side=1 if trade_msg["side"] == "buy" else -1,
                price=float(trade_msg["price"]),
                amount=float(trade_msg["amount"]),
                order_id=str(trade_msg["order_id"]),
                order_type=trade_msg["type"],  # "limit" or "market" or "stop"
                trade_id=str(trade_msg["trade_id"]),
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
                "position_side": pos["position_side"],
                "vwap": float(pos["average_price"]),
                "open_amount": float(pos["open"]),
                "locked_amount": float(pos["locked"]),
                "unrealized_fee_amount": float(pos["unrealized_fee"]),
                "unrealized_interest_amount": float(pos["unrealized_interest"]),
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

if __name__ == "__main__":

    async def main():
        publisher = LocalWsPublisher(port=8765)
        client = BitbankPrivateStreamClient(publisher=publisher)
        client.logger.setLevel(logging.DEBUG)

        async def heartbeat():
            while True:
                msg = {"_data_type": "Heartbeat"}
                await publisher.send(msg)
                await asyncio.sleep(10)

        try:
            await asyncio.gather(
                publisher.start(),
                client.start(),
                heartbeat(),
            )
            await asyncio.sleep(60 * 10)
        except KeyboardInterrupt:
            print("Disconnected")
            asyncio.run(client.close())
    
    asyncio.run(main())
