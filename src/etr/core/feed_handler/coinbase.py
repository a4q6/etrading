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


class CoinbaseSocketClient:
    """
    Websocket feed handler for Coinbase Advanced Trade API.
    https://docs.cdp.coinbase.com/advanced-trade/docs/ws-overview

    ccy_pairs: Coinbase product_id format, e.g. ["BTC-USD", "ETH-USD"]
    Internal key (ticker_plant / market_book keys): "BTCUSD", "ETHUSD"
    """

    WS_URL = "wss://advanced-trade-ws.coinbase.com"

    def __init__(
        self,
        ccy_pairs: List[str] = ["BTC-USD"],
        reconnect_attempts: Optional[int] = None,
        publisher: Optional[LocalWsPublisher] = None,
    ):
        self.ccy_pairs_raw = ccy_pairs  # ["BTC-USD", ...]
        self.publisher = publisher

        # "BTC-USD" -> "BTCUSD"
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
            key: MarketBook(sym=key, venue=VENUE.COINBASE, category="advanced-trade", misc="null")
            for key in keys
        }
        self.rate: Dict[str, Rate] = {
            key: Rate(sym=key, venue=VENUE.COINBASE, category="advanced-trade", misc="null")
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
        # max_size=None: Coinbase level2 snapshots can exceed the default frame size limit
        async with websockets.connect(self.WS_URL, max_size=None) as websocket:
            self._connected = True
            self._ws = websocket

            # subscribe level2 (order book) and market_trades
            sub_level2 = {
                "type": "subscribe",
                "product_ids": self.ccy_pairs_raw,
                "channel": "level2",
            }
            sub_trades = {
                "type": "subscribe",
                "product_ids": self.ccy_pairs_raw,
                "channel": "market_trades",
            }
            # heartbeats keep the connection alive
            sub_hb = {
                "type": "subscribe",
                "product_ids": self.ccy_pairs_raw,
                "channel": "heartbeats",
            }
            await websocket.send(json.dumps(sub_level2))
            self.logger.info(f"Subscribed level2: {self.ccy_pairs_raw}")
            await asyncio.sleep(0.5)
            await websocket.send(json.dumps(sub_trades))
            self.logger.info(f"Subscribed market_trades: {self.ccy_pairs_raw}")
            await asyncio.sleep(0.5)
            await websocket.send(json.dumps(sub_hb))

            try:
                while self._connected and self._running:
                    raw_msg = await websocket.recv()
                    message = json.loads(raw_msg)
                    await self._process_message(message)
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

    # ------------------------------------------------------------------
    # Message processing
    # ------------------------------------------------------------------

    async def _process_message(self, message: dict):
        channel = message.get("channel")

        if channel == "heartbeats":
            return

        if channel == "subscriptions":
            self.logger.info(f"Subscription confirmed: {message}")
            return

        if channel == "l2_data":
            for event in message.get("events", []):
                if event["type"] == "snapshot":
                    await self._on_l2_snapshot(event)
                elif event["type"] == "update":
                    await self._on_l2_update(event)

        elif channel == "market_trades":
            for event in message.get("events", []):
                if event["type"] in ("snapshot", "update"):
                    await self._on_trades(event)

    async def _on_l2_snapshot(self, event: dict):
        """
        event = {
            "type": "snapshot",
            "product_id": "BTC-USD",
            "updates": [
                {"side": "bid", "event_time": "...", "price_level": "60000", "new_quantity": "0.5"},
                {"side": "offer", "event_time": "...", "price_level": "60001", "new_quantity": "0.3"},
                ...
            ]
        }
        'offer' means ask side.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        product_id = event["product_id"]
        key = self._to_key.get(product_id)
        if key is None:
            return

        bids: SortedDict = SortedDict()
        asks: SortedDict = SortedDict()
        mkt_ts = now
        for upd in event.get("updates", []):
            price = float(upd["price_level"])
            qty = float(upd["new_quantity"])
            side = upd["side"]
            if side == "bid":
                if qty > 0:
                    bids[price] = qty
            else:  # "offer"
                if qty > 0:
                    asks[price] = qty
            # take timestamp from the first update entry
            if upd.get("event_time"):
                mkt_ts = _parse_ts(upd["event_time"])

        cur_book = self.market_book[key]
        cur_book.timestamp = now
        cur_book.market_created_timestamp = mkt_ts
        cur_book.bids = bids
        cur_book.asks = asks
        cur_book.universal_id = uuid4().hex
        cur_book.misc = "whole"

        if self.publisher is not None:
            asyncio.create_task(self.publisher.send(cur_book.to_dict()))
        asyncio.create_task(self.ticker_plant[key].info(json.dumps(cur_book.to_dict())))

        await self._emit_rate_from_book(key, cur_book)

    async def _on_l2_update(self, event: dict):
        """
        event = {
            "type": "update",
            "product_id": "BTC-USD",
            "updates": [
                {"side": "bid", "event_time": "...", "price_level": "60000", "new_quantity": "0"},
                ...
            ]
        }
        new_quantity == "0" means remove that price level.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        product_id = event["product_id"]
        key = self._to_key.get(product_id)
        if key is None:
            return

        cur_book = self.market_book[key]
        if cur_book.misc == "null":
            # snapshot not yet received, skip diff
            return

        mkt_ts = now
        for upd in event.get("updates", []):
            price = float(upd["price_level"])
            qty = float(upd["new_quantity"])
            side = upd["side"]
            if upd.get("event_time"):
                mkt_ts = _parse_ts(upd["event_time"])
            if side == "bid":
                if qty == 0.0:
                    cur_book.bids.pop(price, None)
                else:
                    cur_book.bids[price] = qty
            else:  # "offer"
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
        # throttle TP log to 250ms
        if self.last_emit_market_book[key] + datetime.timedelta(milliseconds=250) < now:
            asyncio.create_task(self.ticker_plant[key].info(json.dumps(cur_book.to_dict())))
            self.last_emit_market_book[key] = now

        await self._emit_rate_from_book(key, cur_book)

    async def _on_trades(self, event: dict):
        """
        event = {
            "type": "update",
            "trades": [{
                "trade_id": "12345",
                "product_id": "BTC-USD",
                "price": "1260.01",
                "size": "1.23",
                "side": "BUY" | "SELL",
                "time": "2019-08-14T20:42:27.265Z"
            }]
        }
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        for trade in event.get("trades", []):
            product_id = trade["product_id"]
            key = self._to_key.get(product_id)
            if key is None:
                continue
            mkt_ts = _parse_ts(trade.get("time"))
            side = +1 if trade["side"].upper() == "BUY" else -1
            data = MarketTrade(
                timestamp=now,
                market_created_timestamp=mkt_ts,
                sym=key,
                venue=VENUE.COINBASE,
                category="advanced-trade",
                side=side,
                price=float(trade["price"]),
                amount=float(trade["size"]),
                trade_id=str(trade["trade_id"]),
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

def _parse_ts(ts_str: Optional[str]) -> datetime.datetime:
    """Parse ISO8601 timestamp string to UTC datetime."""
    if not ts_str:
        return datetime.datetime.now(datetime.timezone.utc)
    try:
        return datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except Exception:
        return datetime.datetime.now(datetime.timezone.utc)


if __name__ == "__main__":
    client = CoinbaseSocketClient(ccy_pairs=["BTC-USD", "ETH-USD"])
    try:
        asyncio.run(client.start())
    except KeyboardInterrupt:
        print("Disconnected")
        asyncio.run(client.close())
