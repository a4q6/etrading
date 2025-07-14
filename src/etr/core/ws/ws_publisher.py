import asyncio
import json
import uuid
import websockets
from typing import Dict, List, Any
from pathlib import Path

from etr.config import Config
from etr.common.logger import LoggerFactory


class LocalWsPublisher:
    _instance = None

    def __new__(cls, port):
        if cls._instance is None:
            cls._instance = super(LocalWsPublisher, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, port: int = 8765):
        if self._initialized:
            return
        log_file = Path(Config.LOG_DIR).joinpath("publisher.log").as_posix()
        self.logger = LoggerFactory().get_logger(logger_name=__name__, log_file=log_file)
        self.port = port
        self._server = None
        self._clients: Dict[str, websockets.WebSocketServerProtocol] = {}  # client_id -> websocket
        self._subscriptions: Dict[str, List[Dict[str, str]]] = {}          # client_id -> list of filters
        self._client_ids: Dict[websockets.WebSocketServerProtocol, str] = {}  # reverse mapping
        self._initialized = True

    async def start(self):
        self._server = await websockets.serve(self._handler, "localhost", self.port)
        self.logger.info(f"LocalWsPublisher running on port {self.port}")

    async def _handler(self, websocket: websockets.WebSocketServerProtocol):
        client_id = uuid.uuid4().hex
        self._clients[client_id] = websocket
        self._subscriptions[client_id] = []
        self._client_ids[websocket] = client_id

        # Notify client of their assigned client_id
        try:
            await websocket.send(json.dumps({"client_id": client_id}))
        except Exception:
            await websocket.close()
            return

        self.logger.info(f"Local websocket ClientID = {client_id} connected.")

        try:
            async for message in websocket:
                await self._process_message(client_id, message)
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            await self._remove_client(websocket)

    async def _process_message(self, client_id: str, message: str):
        try:
            data = json.loads(message)
            if data["action"] == "subscribe":
                self._subscriptions[client_id].extend(data["channels"])
            elif data["action"] == "unsubscribe":
                for unsub in data["channels"]:
                    self._subscriptions[client_id] = [
                        sub for sub in self._subscriptions[client_id]
                        if not self._match(sub, unsub)
                    ]
        except Exception as e:
            self.logger.warning(f"Failed to process message from {client_id}: {e}")

    async def send(self, message: Dict[str, Any]):
        dead_clients = []
        for client_id, websocket in self._clients.items():
            try:
                if any(self._match(message, sub) for sub in self._subscriptions.get(client_id, [])):
                    await websocket.send(json.dumps(message))
            except websockets.exceptions.ConnectionClosed:
                dead_clients.append(websocket)
            except Exception as e:
                self.logger.warning(f"Unexpected error for {client_id}: {e}")
                dead_clients.append(websocket)

        for ws in dead_clients:
            await self._remove_client(ws)

    def _match(self, message: Dict[str, str], sub: Dict[str, str]) -> bool:
        return all(
            sub.get(k, "*") == "*" or sub[k] == message.get(k, "")
            for k in ("_data_type", "venue", "sym")
        )

    async def _remove_client(self, websocket: websockets.WebSocketServerProtocol):
        client_id = self._client_ids.pop(websocket, None)
        if client_id:
            self.logger.info(f"Local websocket ClientID = {client_id} disconnected")
            self._clients.pop(client_id, None)
            self._subscriptions.pop(client_id, None)
        try:
            await websocket.close()
        except Exception:
            pass

    async def close(self):
        for ws in list(self._clients.values()):
            try:
                await ws.close()
            except Exception:
                pass
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        LocalWsPublisher._instance = None
        self.logger.info(f"LocalWsPublisher on port {self.port} closed.")

    def __del__(self):
        try:
            asyncio.create_task(self.close())
        except Exception:
            pass


if __name__ == '__main__':

    publisher = LocalWsPublisher(port=8765)
    
    async def data_producer():
        """一定間隔でPublisherにデータを送信する例"""
        import random
        import datetime
        symbols = ["BTCJPY", "ETHJPY"]
        venues = ["bitflyer", "binance"]
        tables = ["MarketBook", "MarketTrade"]

        while True:
            msg = {
                "_data_type": random.choice(tables),
                "venue": random.choice(venues),
                "sym": random.choice(symbols),
                "price": round(random.uniform(30000, 60000), 2),
                "volume": round(random.uniform(0.1, 5.0), 2),
                "ts": datetime.datetime.now().isoformat(),
            }
            await publisher.send(msg)
            await asyncio.sleep(1)

    async def main():
        await publisher.start()
        asyncio.create_task(data_producer())
        while True:
            await asyncio.sleep(10)

    asyncio.run(main())
