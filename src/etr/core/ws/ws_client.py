import asyncio
import json
import websockets
import pandas as pd
from typing import Callable, List, Dict, Optional, Union, Set, FrozenSet, Awaitable
from pathlib import Path

from etr.common.logger import LoggerFactory
from etr.config import Config


class LocalWsClient:
    def __init__(
        self,
        uri: str = "ws://localhost:8765",
        callbacks: List[Awaitable[None]] = [],
        log_file = None,
        reconnect: bool = True,
    ):
        if log_file is not None:
            log_file = Path(Config.LOG_DIR).joinpath("main.log").as_posix()
        self.logger = LoggerFactory().get_logger(logger_name=__name__, log_file=log_file)
        self.uri = uri
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.callbacks = callbacks
        self._running = True
        self.reconnect = reconnect
        self.client_id: Optional[str] = None
        self._subscriptions: Set[FrozenSet[tuple]] = set()

    def register_callback(self, callback: Awaitable[None]):
        self.callbacks.append(callback)

    async def connect(self):
        asyncio.create_task(self._run_forever())

    async def _run_forever(self):
        retry_delay = 1
        while self._running:
            try:
                self.logger.info(f"Connecting to {self.uri}...")
                self.websocket = await websockets.connect(self.uri)

                first_msg = await asyncio.wait_for(self.websocket.recv(), timeout=5)
                first_data = json.loads(first_msg)
                self.client_id = first_data.get("client_id")
                self.logger.info(f"Connected with client_id: {self.client_id}")

                if self._subscriptions:
                    await self._resubscribe()

                retry_delay = 1
                await self._listen()
            
            except RuntimeError as e:
                self.logger.error("RuntimeError detected. Stop connection.")
                await self.close()

            except Exception as e:
                self.logger.warning(f"Connection error: {e}")
                await self._cleanup()

                if not self.reconnect:
                    break

                self.logger.info(f"Reconnecting in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

    def _dict_to_frozenset(self, d: Dict[str, str]) -> FrozenSet[tuple]:
        return frozenset(d.items())

    def _frozenset_to_dict(self, fs: FrozenSet[tuple]) -> Dict[str, str]:
        return dict(fs)

    async def subscribe(self, channels: List[Dict[str, str]]):
        new_channels = [self._dict_to_frozenset(c) for c in channels]
        added = [c for c in new_channels if c not in self._subscriptions]
        if not added:
            return
        self._subscriptions.update(added)
        await self._send({
            "action": "subscribe",
            "channels": [self._frozenset_to_dict(c) for c in added]
        })

    async def unsubscribe(self, channels: List[Dict[str, str]]):
        to_remove = [self._dict_to_frozenset(c) for c in channels]
        removed = [c for c in to_remove if c in self._subscriptions]
        if not removed:
            return
        self._subscriptions.difference_update(removed)
        await self._send({
            "action": "unsubscribe",
            "channels": [self._frozenset_to_dict(c) for c in removed]
        })

    async def _resubscribe(self):
        self.logger.info(f"Resubscribing to {len(self._subscriptions)} channels...")
        await self._send({
            "action": "subscribe",
            "channels": [self._frozenset_to_dict(c) for c in self._subscriptions]
        })

    async def _send(self, message: Dict):
        if self.websocket is None or self.websocket.closed:
            self.logger.info("WebSocket not available. Message not sent.")
            return
        try:
            await self.websocket.send(json.dumps(message))
        except Exception as e:
            self.logger.info(f"Failed to send message: {e}")

    async def _listen(self):
        try:
            async for message in self.websocket:
                try:
                    data = json.loads(message)
                    data = {k: v if "time" not in k else pd.Timestamp(v) for k, v in data.items()}  # parse timestamp values
                    for callback in self.callbacks:
                        await callback(data)
                except json.JSONDecodeError:
                    self.logger.info(f"Invalid JSON received: {message}")
        except websockets.exceptions.ConnectionClosed:
            self.logger.info("Connection closed.")
        finally:
            await self._cleanup()

    async def _cleanup(self):
        if self.websocket:
            try:
                await self.websocket.close()
            except:
                pass
        self.websocket = None

    async def close(self):
        self._running = False
        if self.websocket:
            await self.websocket.close()
        self.logger.info("Closed WebSocket client.")


if __name__ == '__main__':
    import datetime
    async def async_callback(msg: Dict):
        print(f"{datetime.datetime.now()} || Async callback got:", msg)

    async def main():
        client = LocalWsClient(uri="ws://localhost:8765")
        client.register_callback(async_callback)
        await client.connect()

        await asyncio.sleep(2)
        await client.subscribe([
            {"_data_type": "MarketTrade", "venue": "binance", "sym": "BTCUSDT"},
            {"_data_type": "MarketTrade", "venue": "bitflyer", "sym": "FXBTCJPY"},
            {"_data_type": "Rate", "venue": "gmo", "sym": "USDJPY"},
        ])

        await asyncio.sleep(30)
        await client.close()

    asyncio.run(main())
