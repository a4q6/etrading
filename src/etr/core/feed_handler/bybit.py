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


class _BybitSocketClientBase:
    """
    Base class shared by BybitLinearSocketClient and BybitSpotSocketClient.
    https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
    https://bybit-exchange.github.io/docs/v5/websocket/public/trade

    ccy_pairs: Bybit symbol format, e.g. ["BTCUSDT", "ETHUSDT"]
    Internal key: same as symbol (already no separator).
    """

    WS_URL: str = ""
    _VENUE: str = ""
    _CATEGORY: str = ""
    BOOK_DEPTH: int = 50

    def __init__(
        self,
        ccy_pairs: List[str] = ["BTCUSDT"],
        reconnect_attempts: Optional[int] = None,
        publisher: Optional[LocalWsPublisher] = None,
    ):
        self.ccy_pairs = [p.upper().replace("-", "").replace("_", "") for p in ccy_pairs]
        self.publisher = publisher

        # logger
        log_file = Path(Config.LOG_DIR).joinpath("main.log").as_posix()
        tp_file = Path(Config.TP_DIR)
        self.ticker_plant: Dict[str, AsyncBufferedLogger] = {
            sym: AsyncBufferedLogger(
                logger_name=f"TP-{self.__class__.__name__}-{sym}",
                log_dir=tp_file.as_posix(),
            )
            for sym in self.ccy_pairs
        }
        self.logger = LoggerFactory().get_logger(logger_name="main", log_file=log_file)

        # status flags
        self._ws = None
        self._connected = False
        self._running = True
        self.reconnect_attempts = reconnect_attempts

        # market data state
        self.market_book: Dict[str, MarketBook] = {
            sym: MarketBook(sym=sym, venue=self._VENUE, category=self._CATEGORY, misc="null")
            for sym in self.ccy_pairs
        }
        self.rate: Dict[str, Rate] = {
            sym: Rate(sym=sym, venue=self._VENUE, category=self._CATEGORY, misc="null")
            for sym in self.ccy_pairs
        }
        self.last_emit_market_book: Dict[str, datetime.datetime] = {
            sym: datetime.datetime(2000, 1, 1, tzinfo=pytz.utc)
            for sym in self.ccy_pairs
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
        # max_size=None: spot snapshots can exceed the websockets default frame limit
        async with websockets.connect(
            self.WS_URL,
            ping_interval=30,
            ping_timeout=30,
            max_queue=None,
        ) as ws:
            self._connected = True
            self._ws = ws

            # subscribe orderbook + publicTrade for all symbols in one request
            args = []
            for sym in self.ccy_pairs:
                args.append(f"orderbook.{self.BOOK_DEPTH}.{sym}")
                args.append(f"publicTrade.{sym}")
            await ws.send(json.dumps({"op": "subscribe", "args": args}))
            self.logger.info(f"Subscribed: {args}")

            try:
                asyncio.create_task(self._heartbeat(ws))
                while self._connected and self._running:
                    raw = await ws.recv()
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

    async def _heartbeat(self, ws, interval: int = 20):
        """Send ping every `interval` seconds. Bybit disconnects after ~20s of silence."""
        while self._connected and self._running:
            await asyncio.sleep(interval)
            if self._connected and self._running:
                try:
                    await ws.send(json.dumps({"op": "ping"}))
                except Exception:
                    break

    # ------------------------------------------------------------------
    # Message processing
    # ------------------------------------------------------------------

    async def _process_message(self, msg: dict):
        op = msg.get("op")
        if op in ("pong", "subscribe", "ping"):
            # log subscription success/failure explicitly so silent rejections are visible
            if op == "subscribe":
                if msg.get("success"):
                    self.logger.info(f"[{self.__class__.__name__}] Subscription confirmed (conn_id={msg.get('conn_id')})")
                else:
                    self.logger.error(f"[{self.__class__.__name__}] Subscription REJECTED: {msg.get('ret_msg')} | {msg}")
            return
        # server-side messages without a topic (e.g. pong with ret_msg)
        if "ret_msg" in msg and "topic" not in msg:
            return

        topic = msg.get("topic", "")

        if "orderbook" in topic:
            sym = topic.split(".")[-1]
            msg_type = msg.get("type")
            if msg_type == "snapshot":
                await self._on_book_snapshot(sym, msg)
            elif msg_type == "delta":
                await self._on_book_delta(sym, msg)
            else:
                self.logger.warning(f"[{self.__class__.__name__}] Unknown orderbook msg_type={msg_type!r} for {sym}")

        elif "publicTrade" in topic:
            sym = topic.split(".")[-1]
            await self._on_trade(sym, msg)

        elif topic:
            self.logger.warning(f"[{self.__class__.__name__}] Unhandled topic: {topic}")
        else:
            self.logger.warning(f"[{self.__class__.__name__}] Message without topic: {msg}")

    async def _on_book_snapshot(self, sym: str, msg: dict):
        """
        msg["data"] = {
            "s": "BTCUSDT",
            "b": [["price_str", "qty_str"], ...],  # bids
            "a": [["price_str", "qty_str"], ...],  # asks
            "u": <update_id>,
            "seq": <sequence>
        }
        """
        if sym not in self.market_book:
            return
        now = datetime.datetime.now(datetime.timezone.utc)
        mkt_ts = _ms_to_dt(msg.get("ts"))
        data = msg["data"]

        cur_book = self.market_book[sym]
        cur_book.timestamp = now
        cur_book.market_created_timestamp = mkt_ts
        cur_book.bids = SortedDict({float(b[0]): float(b[1]) for b in data["b"] if float(b[1]) > 0})
        cur_book.asks = SortedDict({float(a[0]): float(a[1]) for a in data["a"] if float(a[1]) > 0})
        cur_book.universal_id = uuid4().hex
        cur_book.misc = "whole"

        if self.publisher is not None:
            asyncio.create_task(self.publisher.send(cur_book.to_dict()))
        asyncio.create_task(self.ticker_plant[sym].info(json.dumps(cur_book.to_dict())))

        await self._emit_rate_from_book(sym, cur_book)

    async def _on_book_delta(self, sym: str, msg: dict):
        """
        msg["data"] = {
            "s": "BTCUSDT",
            "b": [["price_str", "0"], ...],   # qty "0" => remove
            "a": [...],
            "u": <update_id>,
            "seq": <sequence>
        }
        """
        if sym not in self.market_book:
            return
        cur_book = self.market_book[sym]
        if cur_book.misc == "null":
            # snapshot not yet received, discard diff
            return

        now = datetime.datetime.now(datetime.timezone.utc)
        mkt_ts = _ms_to_dt(msg.get("ts"))
        data = msg["data"]

        for b in data.get("b", []):
            price, qty = float(b[0]), float(b[1])
            if qty == 0.0:
                cur_book.bids.pop(price, None)
            else:
                cur_book.bids[price] = qty

        for a in data.get("a", []):
            price, qty = float(a[0]), float(a[1])
            if qty == 0.0:
                cur_book.asks.pop(price, None)
            else:
                cur_book.asks[price] = qty

        cur_book.timestamp = now
        cur_book.market_created_timestamp = mkt_ts
        cur_book.universal_id = uuid4().hex
        cur_book.misc = "diff"

        if self.publisher is not None:
            asyncio.create_task(self.publisher.send(cur_book.to_dict()))
        if self.last_emit_market_book[sym] + datetime.timedelta(milliseconds=250) < now:
            asyncio.create_task(self.ticker_plant[sym].info(json.dumps(cur_book.to_dict())))
            self.last_emit_market_book[sym] = now

        await self._emit_rate_from_book(sym, cur_book)

    async def _on_trade(self, sym: str, msg: dict):
        """
        msg["data"] = [{
            "T": <timestamp_ms>,
            "s": "BTCUSDT",
            "S": "Buy" | "Sell",
            "v": "0.001",   # size
            "p": "16578.50",
            "i": "2100000000031220",  # trade_id
            "BT": false     # is block trade
        }, ...]
        """
        if sym not in self.ticker_plant:
            return
        now = datetime.datetime.now(datetime.timezone.utc)
        for t in msg.get("data", []):
            mkt_ts = _ms_to_dt(t.get("T"))
            side = +1 if t["S"] == "Buy" else -1
            data = MarketTrade(
                timestamp=now,
                market_created_timestamp=mkt_ts,
                sym=sym,
                venue=self._VENUE,
                category=self._CATEGORY,
                side=side,
                price=float(t["p"]),
                amount=float(t["v"]),
                trade_id=str(t["i"]),
                misc="block_trade" if t.get("BT") else None,
            )
            if self.publisher is not None:
                asyncio.create_task(self.publisher.send(data.to_dict()))
            asyncio.create_task(self.ticker_plant[sym].info(json.dumps(data.to_dict())))

    async def _emit_rate_from_book(self, sym: str, book: MarketBook):
        new_rate = book.to_rate()
        if new_rate.mid_price != self.rate[sym].mid_price or self.rate[sym].misc == "null":
            self.rate[sym] = new_rate
            if self.publisher is not None:
                asyncio.create_task(self.publisher.send(new_rate.to_dict()))
            asyncio.create_task(self.ticker_plant[sym].info(json.dumps(new_rate.to_dict())))


# ------------------------------------------------------------------
# Concrete subclasses
# ------------------------------------------------------------------

class BybitLinearSocketClient(_BybitSocketClientBase):
    """Bybit USDT Perpetual (linear) feed handler."""

    WS_URL = "wss://stream.bybit.com/v5/public/linear"
    _VENUE = VENUE.BYBIT_LINEAR
    _CATEGORY = "linear"


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
    client = BybitLinearSocketClient(ccy_pairs=["BTCUSDT", "ETHUSDT"])
    try:
        asyncio.run(client.start())
    except KeyboardInterrupt:
        print("Disconnected")
        asyncio.run(client.close())
