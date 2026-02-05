import asyncio
import datetime
import json
import pandas as pd
import time
import logging
from asyncio import Lock
from uuid import uuid4
from typing import Optional, Dict, List
from pathlib import Path

import websockets
import aiohttp

from etr.core.async_logger import AsyncBufferedLogger
from etr.core.datamodel import VENUE, Order, Trade, OrderStatus, OrderType
from etr.core.ws import LocalWsPublisher
from etr.config import Config
from etr.common.logger import LoggerFactory


class GmoPrivateStreamClient:
    """
    GMO Coin Private WebSocket API Client
    https://api.coin.z.com/docs/

    Private channels:
    - executionEvents: Execution (trade) notifications
    - orderEvents: Order notifications
    - positionEvents: Position notifications
    - positionSummaryEvents: Position summary notifications
    """

    WS_URL = "wss://api.coin.z.com/ws/private"
    REST_URL = "https://api.coin.z.com/private"

    def __init__(
        self,
        publisher: Optional[LocalWsPublisher] = None,
        api_key: str = Config.GMO_API_KEY,
        api_secret: str = Config.GMO_API_SECRET,
        tp_number: int = 0,
        symbols: List[str] = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbols = symbols or ["BTC_JPY", "ETH_JPY"]  # Default symbols to subscribe
        self._running = False
        self._ws = None
        self._reconnect_lock = Lock()
        self._last_reconnect_time = 0
        self._access_token = None
        self._token_expire_time = None

        # logger
        log_file = Path(Config.LOG_DIR).joinpath("main.log").as_posix()
        tp_file = Path(Config.TP_DIR)
        self.ticker_plant = AsyncBufferedLogger(
            logger_name=f"TP-GmoPrivate{tp_number}-ALL",
            log_dir=tp_file.as_posix()
        )
        self.logger = LoggerFactory().get_logger(logger_name="main", log_file=log_file)
        self.publisher = publisher

    def _generate_signature(self, method: str, path: str, body: str = ""):
        """Generate authentication headers for GMO API."""
        import hmac
        import hashlib
        timestamp = str(int(time.time() * 1000))
        text = timestamp + method.upper() + path + body
        signature = hmac.new(
            self.api_secret.encode(),
            text.encode(),
            hashlib.sha256
        ).hexdigest()
        headers = {
            "API-KEY": self.api_key,
            "API-TIMESTAMP": timestamp,
            "API-SIGN": signature,
            "Content-Type": "application/json"
        }
        return headers

    async def _get_access_token(self) -> str:
        """Get access token for Private WebSocket API."""
        path = "/v1/ws-auth"
        headers = self._generate_signature("POST", path, "")
        url = self.REST_URL + path

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers) as resp:
                result = await resp.json()
                if result.get("status") == 0 and result.get("data"):
                    self._access_token = result["data"]
                    self._token_expire_time = datetime.datetime.now() + datetime.timedelta(minutes=55)
                    self.logger.info(f"Got access token: {self._access_token[:20]}...")
                    return self._access_token
                else:
                    raise RuntimeError(f"Failed to get access token: {result}")

    async def _extend_access_token(self) -> bool:
        """Extend access token validity."""
        if not self._access_token:
            return False

        path = "/v1/ws-auth"
        body = json.dumps({"token": self._access_token})
        headers = self._generate_signature("PUT", path, body)
        url = self.REST_URL + path

        async with aiohttp.ClientSession() as session:
            async with session.put(url, headers=headers, data=body) as resp:
                result = await resp.json()
                if result.get("status") == 0:
                    self._token_expire_time = datetime.datetime.now() + datetime.timedelta(minutes=55)
                    self.logger.info("Extended access token")
                    return True
                else:
                    self.logger.warning(f"Failed to extend access token: {result}")
                    return False

    async def start(self):
        """Start the WebSocket connection and event processing."""
        self._running = True
        asyncio.create_task(self._run())
        asyncio.create_task(self._token_refresh_loop())

    async def close(self):
        """Close the WebSocket connection."""
        self._running = False
        if self._ws:
            await self._ws.close()

    async def _token_refresh_loop(self):
        """Periodically refresh access token."""
        while self._running:
            await asyncio.sleep(60)
            if self._token_expire_time and datetime.datetime.now() > self._token_expire_time - datetime.timedelta(minutes=5):
                try:
                    await self._extend_access_token()
                except Exception as e:
                    self.logger.error(f"Failed to extend token: {e}")

    async def _run(self):
        """Main loop for WebSocket connection."""
        while self._running:
            try:
                await self._connect_and_process()
            except websockets.ConnectionClosed as e:
                self.logger.warning(f"WebSocket connection closed: {e}")
                if self._running:
                    await asyncio.sleep(5)
            except Exception as e:
                self.logger.error(f"WebSocket error: {e}", exc_info=True)
                if self._running:
                    await asyncio.sleep(5)

    async def _connect_and_process(self):
        """Connect to WebSocket, subscribe, and process messages."""
        # Get access token first
        await self._get_access_token()

        self.logger.info(f"Connecting to GMO WebSocket: {self.WS_URL}")

        async with websockets.connect(self.WS_URL + f"/v1/{self._access_token}") as ws:
            self._ws = ws
            self.logger.info("WebSocket connected")

            # Subscribe to private channels
            for channel in ["executionEvents", "orderEvents", "positionEvents"]:
                await self._subscribe(ws, channel)

            # Process messages
            async for message in ws:
                try:
                    data = json.loads(message)
                    await self._handle_message(data)
                except json.JSONDecodeError:
                    self.logger.error(f"Failed to parse message: {message}")
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}", exc_info=True)

    async def _subscribe(self, ws, channel: str):
        """Subscribe to a channel."""
        subscribe_request = {
            "command": "subscribe",
            "channel": channel,
            "accessToken": self._access_token,
        }
        self.logger.info(f"Subscribing to channel: {channel}")
        await ws.send(json.dumps(subscribe_request))
        await asyncio.sleep(1)  # Rate limit: 1 request per second

    async def _handle_message(self, data: Dict):
        """Handle incoming WebSocket message."""
        channel = data.get("channel")

        if channel == "executionEvents":
            await self._handle_execution_events(data)
        elif channel == "orderEvents":
            await self._handle_order_events(data)
        elif channel == "positionEvents":
            await self._handle_position_events(data)
        elif channel == "positionSummaryEvents":
            await self._handle_position_summary_events(data)
        else:
            self.logger.debug(f"Received: {data}")

    async def _handle_execution_events(self, data: Dict):
        """
        Handle executionEvents messages (trade notifications).

        Expected format:
        {
            "channel": "executionEvents",
            "executionId": 123456,
            "orderId": 789012,
            "symbol": "BTC_JPY",
            "side": "BUY",
            "price": "10000000",
            "size": "0.01",
            "timestamp": "2024-01-01T00:00:00.000Z",
            ...
        }
        """
        self.logger.debug(f"executionEvent: {data}")

        # Parse timestamp
        timestamp_str = data.get("timestamp")
        if timestamp_str:
            mkt_time = pd.to_datetime(timestamp_str).tz_convert("UTC")
        else:
            mkt_time = datetime.datetime.now(datetime.timezone.utc)

        sym = data.get("symbol", "").replace("_", "").upper()

        # Build misc info
        misc = {
            "lossGain": data.get("lossGain"),
            "fee": data.get("fee"),
            "settleType": data.get("settleType"),
            "channel": "streaming-executionEvents",
        }

        trade = Trade(
            timestamp=datetime.datetime.now(datetime.timezone.utc),
            market_created_timestamp=mkt_time,
            sym=sym,
            venue=VENUE.GMO,
            side=1 if data.get("side") == "BUY" else -1,
            price=float(data.get("price", 0)),
            amount=float(data.get("size", 0)),
            order_id=str(data.get("orderId", "")),
            order_type=data.get("executionType", "").lower() if data.get("executionType") else "",
            trade_id=str(data.get("executionId", uuid4().hex)),
            misc=str(misc),
        )

        # Send to publisher
        if self.publisher is not None:
            asyncio.create_task(self.publisher.send(trade.to_dict()))
        asyncio.create_task(self.ticker_plant.info(json.dumps(trade.to_dict())))

    async def _handle_order_events(self, data: Dict):
        """
        Handle orderEvents messages (order notifications).

        Expected format:
        {
            "channel": "orderEvents",
            "orderId": 123456,
            "symbol": "BTC_JPY",
            "side": "BUY",
            "executionType": "LIMIT",
            "price": "10000000",
            "size": "0.01",
            "status": "ORDERED",
            "timestamp": "2024-01-01T00:00:00.000Z",
            ...
        }
        """
        self.logger.debug(f"orderEvent: {data}")

        # Parse timestamp
        timestamp_str = data.get("timestamp")
        if timestamp_str:
            mkt_time = pd.to_datetime(timestamp_str).tz_convert("UTC")
        else:
            mkt_time = datetime.datetime.now(datetime.timezone.utc)

        # Determine order status
        status = data.get("status", "").upper()
        order_status = self._convert_order_status(status)

        # Determine order type
        exec_type = data.get("executionType", "LIMIT").upper()
        order_type = self._convert_execution_type(exec_type)

        # Determine price
        price = float(data.get("price", 0)) if data.get("price") else 0

        order = Order(
            timestamp=datetime.datetime.now(datetime.timezone.utc),
            market_created_timestamp=mkt_time,
            sym=data.get("symbol", "").replace("_", "").upper(),
            side=1 if data.get("side") == "BUY" else -1,
            price=price,
            amount=float(data.get("size", 0)),
            executed_amount=float(data.get("executedSize", 0)) if data.get("executedSize") else 0,
            order_type=order_type,
            order_status=order_status,
            venue=VENUE.GMO,
            order_id=str(data.get("orderId", "")),
            model_id=None,
            process_id=None,
            src_type=None,
            src_id=None,
            src_timestamp=pd.NaT,
            misc=f"streaming-orderEvents-{status}",
        )

        # Send to publisher
        if self.publisher is not None:
            asyncio.create_task(self.publisher.send(order.to_dict()))
        asyncio.create_task(self.ticker_plant.info(json.dumps(order.to_dict())))

    async def _handle_position_events(self, data: Dict):
        """Handle positionEvents messages."""
        self.logger.debug(f"positionEvent: {data}")

        # Build position update data
        pos_data = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "sym": data.get("symbol", "").replace("_", "").upper(),
            "venue": VENUE.GMO,
            "position_id": str(data.get("positionId", "")),
            "side": data.get("side"),
            "size": float(data.get("size", 0)) if data.get("size") else 0,
            "price": float(data.get("price", 0)) if data.get("price") else 0,
            "lossGain": float(data.get("lossGain", 0)) if data.get("lossGain") else 0,
            "universal_id": uuid4().hex,
            "_data_type": "PositionUpdate",
        }

        if self.publisher is not None:
            asyncio.create_task(self.publisher.send(pos_data))
        asyncio.create_task(self.ticker_plant.info(json.dumps(pos_data)))

    async def _handle_position_summary_events(self, data: Dict):
        """Handle positionSummaryEvents messages."""
        self.logger.debug(f"positionSummaryEvent: {data}")

        pos_data = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "sym": data.get("symbol", "").replace("_", "").upper(),
            "venue": VENUE.GMO,
            "side": data.get("side"),
            "averagePositionRate": float(data.get("averagePositionRate", 0)) if data.get("averagePositionRate") else 0,
            "positionLossGain": float(data.get("positionLossGain", 0)) if data.get("positionLossGain") else 0,
            "sumOrderQuantity": float(data.get("sumOrderQuantity", 0)) if data.get("sumOrderQuantity") else 0,
            "sumPositionQuantity": float(data.get("sumPositionQuantity", 0)) if data.get("sumPositionQuantity") else 0,
            "universal_id": uuid4().hex,
            "_data_type": "PositionSummary",
        }

        if self.publisher is not None:
            asyncio.create_task(self.publisher.send(pos_data))
        asyncio.create_task(self.ticker_plant.info(json.dumps(pos_data)))

    def _convert_order_status(self, status: str) -> OrderStatus:
        """Convert GMO order status to internal status."""
        mapping = {
            "WAITING": OrderStatus.Sent,
            "ORDERED": OrderStatus.Sent,
            "MODIFYING": OrderStatus.Sent,
            "CANCELLING": OrderStatus.Sent,
            "CANCELED": OrderStatus.Canceled,
            "EXECUTED": OrderStatus.Filled,
            "EXPIRED": OrderStatus.Canceled,
        }
        return mapping.get(status.upper(), OrderStatus.Sent)

    def _convert_execution_type(self, exec_type: str) -> str:
        """Convert GMO execution type to internal order type."""
        mapping = {
            "MARKET": OrderType.Market,
            "LIMIT": OrderType.Limit,
            "STOP": OrderType.Stop,
        }
        return mapping.get(exec_type.upper(), OrderType.Limit)


if __name__ == "__main__":

    async def main():
        publisher = LocalWsPublisher(port=8765)
        client = GmoPrivateStreamClient(publisher=publisher)
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
        except KeyboardInterrupt:
            print("Disconnected")
            await client.close()

    asyncio.run(main())
