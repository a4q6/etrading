import asyncio
import websockets
import json
import numpy as np
import datetime
import pytz
from typing import Optional, List, Dict
from sortedcontainers import SortedDict
from uuid import uuid4
from pathlib import Path

from etr.common.logger import LoggerFactory
from etr.core.async_logger import AsyncBufferedLogger
from etr.core.datamodel import MarketTrade, MarketBook, Rate, VENUE
from etr.config import Config
from etr.core.ws import LocalWsPublisher


class OkxSwapSocketClient:
    """
    Websocket feed handler for OKX USDT Perpetual (SWAP).
    https://www.okx.com/docs-v5/en/#websocket-api-public-channel-order-book-channel
    https://www.okx.com/docs-v5/en/#websocket-api-public-channel-trades-channel

    Uses `books5` channel (top 5 levels, always full snapshot) and `trades` channel.
    Ping/pong: send plain text "ping" every 25s; server replies with plain text "pong".

    ccy_pairs: OKX instId format, e.g. ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
    Internal key: "BTCUSDTSWAP", "ETHUSDTSWAP" (hyphens removed)
    """

    WS_URL = "wss://ws.okx.com:8443/ws/v5/public"

    def __init__(
        self,
        ccy_pairs: List[str] = ["BTC-USDT-SWAP"],
        reconnect_attempts: Optional[int] = None,
        publisher: Optional[LocalWsPublisher] = None,
    ):
        self.ccy_pairs_raw = ccy_pairs  # ["BTC-USDT-SWAP", ...]
        self.publisher = publisher

        # "BTC-USDT-SWAP" -> "BTCUSDTSWAP"
        self._to_key: Dict[str, str] = {p: p.replace("-", "") for p in ccy_pairs}

        # logger
        log_file = Path(Config.LOG_DIR).joinpath("main.log").as_posix()
        tp_file = Path(Config.TP_DIR)
        keys = list(self._to_key.values())
        self.ticker_plant: Dict[str, AsyncBufferedLogger] = {
            key: AsyncBufferedLogger(
                logger_name=f"TP-{self.__class__.__name__}-{key}",
                log_dir=tp_file.as_posix(),
            )
            for key in keys
        }
        self.logger = LoggerFactory().get_logger(logger_name="main", log_file=log_file)

        # status flags
        self._ws = None
        self._connected = False
        self._running = True
        self.reconnect_attempts = reconnect_attempts

        # market data state
        self.market_book: Dict[str, MarketBook] = {
            key: MarketBook(sym=key, venue=VENUE.OKX, category="swap", misc="null")
            for key in keys
        }
        self.rate: Dict[str, Rate] = {
            key: Rate(sym=key, venue=VENUE.OKX, category="swap", misc="null")
            for key in keys
        }
        self.last_emit_market_book: Dict[str, datetime.datetime] = {
            key: datetime.datetime(2000, 1, 1, tzinfo=pytz.utc)
            for key in keys
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

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
                sleep_sec = 10 * np.log(max(self.attempts, 1))
                self.logger.info(f"Wait {round(sleep_sec, 2)} seconds to reconnect...")
                await asyncio.sleep(sleep_sec)

    async def _connect(self):
        self.logger.info(f"Connecting to {self.WS_URL}")
        async with websockets.connect(self.WS_URL,
            ping_interval=30,
            ping_timeout=30,
            max_queue=None,
        ) as ws:
            self._connected = True
            self._ws = ws

            # subscribe books5 and trades for all instIds
            args = []
            for inst_id in self.ccy_pairs_raw:
                args.append({"channel": "books5", "instId": inst_id})
                args.append({"channel": "trades", "instId": inst_id})
            await ws.send(json.dumps({"op": "subscribe", "args": args}))
            self.logger.info(f"Subscribed: {self.ccy_pairs_raw}")

            try:
                asyncio.create_task(self._heartbeat(ws))
                while self._connected and self._running:
                    raw = await ws.recv()
                    # OKX ping/pong uses plain text, not JSON
                    if raw == "pong":
                        continue
                    msg = json.loads(raw)
                    await self._process_message(msg)
                    self.attempts = 0

            except websockets.exceptions.ConnectionClosedOK:
                self.logger.info("Websocket closed OK")
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

    async def _heartbeat(self, ws, interval: int = 25):
        """OKX requires plain-text "ping" to keep connection alive (30s timeout)."""
        while self._connected and self._running:
            await asyncio.sleep(interval)
            if self._connected and self._running:
                try:
                    await ws.send("ping")
                except Exception:
                    break

    # ------------------------------------------------------------------
    # Message processing
    # ------------------------------------------------------------------

    async def _process_message(self, msg: dict):
        # subscription confirmation
        event = msg.get("event")
        if event == "subscribe":
            self.logger.info(f"Subscription confirmed: {msg.get('arg')}")
            return
        elif event == "error":
            self.logger.error(f"OKX error: {msg}")
            return

        arg = msg.get("arg", {})
        channel = arg.get("channel")
        inst_id = arg.get("instId")
        key = self._to_key.get(inst_id)
        if key is None:
            return

        if channel == "books5":
            for item in msg.get("data", []):
                await self._on_books5(key, item)

        elif channel == "trades":
            for item in msg.get("data", []):
                await self._on_trade(key, item)

    async def _on_books5(self, key: str, item: dict):
        """
        books5 is always a full snapshot (top 5 price levels on each side).
        item = {
            "bids": [["price", "qty", "0", "num_orders"], ...],
            "asks": [["price", "qty", "0", "num_orders"], ...],
            "ts": "1621027983000",   # ms since epoch
            "seqId": 3
        }
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        mkt_ts = _ms_to_dt(item.get("ts"))

        cur_book = self.market_book[key]
        cur_book.timestamp = now
        cur_book.market_created_timestamp = mkt_ts
        cur_book.bids = SortedDict({float(b[0]): float(b[1]) for b in item["bids"] if float(b[1]) > 0})
        cur_book.asks = SortedDict({float(a[0]): float(a[1]) for a in item["asks"] if float(a[1]) > 0})
        cur_book.universal_id = uuid4().hex
        cur_book.misc = "whole"

        if self.publisher is not None:
            asyncio.create_task(self.publisher.send(cur_book.to_dict()))
        # throttle TP log: books5 can arrive very frequently
        if self.last_emit_market_book[key] + datetime.timedelta(milliseconds=250) < now:
            asyncio.create_task(self.ticker_plant[key].info(json.dumps(cur_book.to_dict())))
            self.last_emit_market_book[key] = now

        await self._emit_rate_from_book(key, cur_book)

    async def _on_trade(self, key: str, item: dict):
        """
        item = {
            "instId": "BTC-USDT-SWAP",
            "tradeId": "130639474",
            "px": "42219.9",
            "sz": "0.12060306",
            "side": "buy" | "sell",
            "ts": "1630048573821"
        }
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        mkt_ts = _ms_to_dt(item.get("ts"))
        side = +1 if item["side"] == "buy" else -1
        data = MarketTrade(
            timestamp=now,
            market_created_timestamp=mkt_ts,
            sym=key,
            venue=VENUE.OKX,
            category="swap",
            side=side,
            price=float(item["px"]),
            amount=float(item["sz"]),
            trade_id=str(item["tradeId"]),
        )
        if self.publisher is not None:
            asyncio.create_task(self.publisher.send(data.to_dict()))
        asyncio.create_task(self.ticker_plant[key].info(json.dumps(data.to_dict())))

    async def _emit_rate_from_book(self, key: str, book: MarketBook):
        new_rate = book.to_rate()
        if new_rate.mid_price != self.rate[key].mid_price or self.rate[key].misc == "null":
            self.rate[key] = new_rate
            if self.publisher is not None:
                asyncio.create_task(self.publisher.send(new_rate.to_dict()))
            asyncio.create_task(self.ticker_plant[key].info(json.dumps(new_rate.to_dict())))


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _ms_to_dt(ts_ms) -> datetime.datetime:
    """Convert Unix timestamp in milliseconds (int or str) to UTC datetime."""
    if ts_ms is None:
        return datetime.datetime.now(datetime.timezone.utc)
    try:
        return datetime.datetime.fromtimestamp(int(ts_ms) / 1e3, tz=datetime.timezone.utc)
    except Exception:
        return datetime.datetime.now(datetime.timezone.utc)


if __name__ == "__main__":
    client = OkxSwapSocketClient(ccy_pairs=["BTC-USDT-SWAP", "ETH-USDT-SWAP"])
    try:
        asyncio.run(client.start())
    except KeyboardInterrupt:
        print("Disconnected")
        asyncio.run(client.close())
