import hmac
import hashlib
import json
import aiohttp
import datetime
import time
import pandas as pd
import asyncio
from typing import Optional, Dict, Union, List
from copy import copy
from pathlib import Path
from uuid import uuid4

from etr.core.api_client.realtime_counter import RealtimeCounter
from etr.core.async_logger import AsyncBufferedLogger
from etr.core.datamodel import VENUE, OrderType, OrderStatus, Order, Trade
from etr.core.api_client.base import ExchangeClientBase
from etr.common.logger import LoggerFactory
from etr.strategy.base_strategy import StrategyBase
from etr.config import Config


# bitFlyer API error codes
BITFLYER_ERROR_CODES = {
    -1: "Unknown error",
    -100: "Request limit exceeded",
    -102: "Insufficient balance",
    -103: "Order is already done",
    -106: "Price is too far from the current price",
    -107: "Order size is too large",
    -108: "Order size is too small",
    -109: "Market order is not allowed",
    -110: "Price should be positive",
    -200: "Internal error",
    -500: "Order not found",
    -501: "Order has already been completed",
    -502: "Order has already been canceled",
}


class BitflyerRestClient(ExchangeClientBase):
    """
    bitFlyer Lightning REST API Client
    https://lightning.bitflyer.com/docs
    """

    BASE_URL = "https://api.bitflyer.com"

    def as_bf_symbol(self, sym: str) -> str:
        """
        Convert internal symbol to bitflyer format.
        BTCJPY -> BTC_JPY, FXBTCJPY -> FX_BTC_JPY
        """
        if sym.startswith("FX"):
            # FXBTCJPY -> FX_BTC_JPY
            return "FX_" + sym[2:-3] + "_" + sym[-3:]
        else:
            # BTCJPY -> BTC_JPY
            return sym[:-3] + "_" + sym[-3:]

    def as_common_symbol(self, sym: str) -> str:
        """
        Convert bitflyer symbol to internal format.
        BTC_JPY -> BTCJPY, FX_BTC_JPY -> FXBTCJPY
        """
        return sym.replace("_", "").upper()

    def __init__(
        self,
        api_key: str = Config.BITFLYER_API_KEY,
        api_secret: str = Config.BITFLYER_API_SECRET,
        log_file: Optional[str] = None,
        tp_number: int = 1,
        **kwargs,
    ):
        self.api_key = api_key
        self.api_secret = api_secret

        # logger
        tp_file = Path(Config.TP_DIR)
        self.ticker_plant = AsyncBufferedLogger(
            logger_name=f"TP-BitFlyerPrivate{tp_number}-ALL",
            log_dir=tp_file.as_posix()
        )
        logger_name = "bf-client" if log_file is None else log_file.split("/")[-1].split(".")[0]
        if log_file is not None:
            log_file = Path(Config.LOG_DIR).joinpath(log_file).as_posix()
        self.logger = LoggerFactory().get_logger(logger_name=logger_name, log_file=log_file)
        self.closed_pnl = 0
        self.open_pnl = {}
        self.positions = {}  # {sym: (vwap, amount)}
        self.pending_positions = False
        self._api_error_count = RealtimeCounter(window_sec=60)

        self._order_cache: Dict[str, Order] = {}
        self._transaction_cache: Dict[str, Trade] = {}
        self.strategy: StrategyBase = None
        self._last_stream_msg_timestamp = pd.NaT
        self._last_order_timestamp = pd.NaT

    def _generate_signature(self, method: str, path: str, body: str = ""):
        """Generate authentication headers for bitFlyer API."""
        timestamp = str(int(time.time() * 1000))
        # signature = HMAC-SHA256(timestamp + method + path + body, secret)
        text = timestamp + method.upper() + path + body
        signature = hmac.new(
            self.api_secret.encode(),
            text.encode(),
            hashlib.sha256
        ).hexdigest()
        headers = {
            "ACCESS-KEY": self.api_key,
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-SIGN": signature,
            "Content-Type": "application/json"
        }
        return headers

    async def _request(
        self,
        method: str,
        path: str,
        params: dict = None,
        body: dict = None,
    ) -> Dict:
        """Make authenticated request to bitFlyer API."""
        url = self.BASE_URL + path

        # Build query string for GET
        if method.upper() == "GET" and params:
            query_string = "&".join(f"{k}={v}" for k, v in params.items())
            path = path + "?" + query_string
            url = self.BASE_URL + path
            body_str = ""
        else:
            body_str = json.dumps(body) if body else ""

        headers = self._generate_signature(method=method.upper(), path=path, body=body_str)

        async with aiohttp.ClientSession() as session:
            if method.upper() == "GET":
                async with session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        error_text = await resp.text()
                        self.logger.error(f"API Error: {resp.status} - {error_text}")
                        return {"success": False, "error": error_text, "status": resp.status}
                    return await resp.json()
            elif method.upper() == "POST":
                async with session.post(url, headers=headers, data=body_str) as resp:
                    if resp.status != 200:
                        error_text = await resp.text()
                        self.logger.error(f"API Error: {resp.status} - {error_text}")
                        return {"success": False, "error": error_text, "status": resp.status}
                    return await resp.json()
            else:
                raise NotImplementedError(f"HTTP method {method} not supported")

    async def send_order(
        self,
        timestamp: datetime.datetime,
        sym: str,  # BTCJPY, FXBTCJPY, etc.
        side: int,  # -1/+1
        price: Optional[float],
        amount: float,
        order_type: OrderType,
        src_type: str,
        src_timestamp: datetime.datetime,
        src_id=None,
        return_raw_response=False,
        misc=None,
        minute_to_expire: int = 43200,
        time_in_force: str = "GTC",
        **kwargs
    ):
        """Send new order to bitFlyer."""
        if pd.notna(self._last_stream_msg_timestamp) and pd.notna(self._last_order_timestamp):
            if self._last_stream_msg_timestamp + datetime.timedelta(seconds=180) < self._last_order_timestamp:
                self.logger.error(
                    f"Latest stream message ({self._last_stream_msg_timestamp}) is too old. "
                    "Streaming might be dead now."
                )
                raise RuntimeError(
                    f"Latest stream message ({self._last_stream_msg_timestamp}) is too old. "
                    "Streaming might be dead now."
                )

        # Create order info
        data = Order(
            datetime.datetime.now(datetime.timezone.utc),
            market_created_timestamp=pd.NaT,
            sym=sym,
            side=side,
            price=price,
            amount=amount,
            executed_amount=0,
            order_type=order_type,
            order_status=OrderStatus.New,
            venue=VENUE.BITFLYER,
            model_id=self.strategy.model_id if self.strategy else None,
            process_id=self.strategy.process_id if self.strategy else None,
            src_type=src_type,
            src_id=src_id,
            src_timestamp=src_timestamp,
            misc=misc,
        )
        asyncio.create_task(self.ticker_plant.info(json.dumps(data.to_dict())))  # store (new)

        # Build request body
        body = {
            "product_code": self.as_bf_symbol(sym),
            "side": "BUY" if side > 0 else "SELL",
            "size": amount,
            "minute_to_expire": minute_to_expire,
            "time_in_force": time_in_force,
        }

        if order_type == OrderType.Limit:
            body["child_order_type"] = "LIMIT"
            body["price"] = int(price)
        elif order_type == OrderType.Market:
            body["child_order_type"] = "MARKET"
        else:
            raise ValueError(f"Unsupported order type: {order_type}")

        # Send request
        res = await self._request("POST", "/v1/me/sendchildorder", body=body)
        if return_raw_response:
            return res

        if "child_order_acceptance_id" not in res:
            self.logger.error(f"Failed to send new order (UUID = {data.universal_id})\n{res}")
            data.order_status = OrderStatus.Canceled
            data.misc = str(res.get("error", res))
            asyncio.create_task(self.ticker_plant.info(json.dumps(data.to_dict())))  # store (canceled)
            self._api_error_count.increment()
            return data

        # Update order info
        acceptance_id = res["child_order_acceptance_id"]
        data.timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        data.order_id = acceptance_id
        data.order_status = OrderStatus.Sent
        data.universal_id = uuid4().hex
        self._order_cache[data.order_id] = data
        asyncio.create_task(self.ticker_plant.info(json.dumps(data.to_dict())))  # store (sent)
        self.logger.info(
            f"Sent new order (order_id, type, side, price, size) = "
            f"({data.order_id}, {data.order_type}, {data.side}, {data.price}, {data.amount})"
        )
        self._last_order_timestamp = datetime.datetime.now()
        return data

    async def cancel_order(
        self,
        order_id: str,
        timestamp,
        src_type,
        src_timestamp,
        src_id=None,
        misc=None,
        sym=None,
        return_raw_response=False,
        **kwargs
    ) -> Order:
        """Cancel an order."""
        if sym is not None:
            product_code = self.as_bf_symbol(sym)
        elif order_id in self._order_cache:
            product_code = self.as_bf_symbol(self._order_cache[order_id].sym)
        else:
            raise ValueError("sym must be provided if order_id is not in cache")

        body = {
            "product_code": product_code,
            "child_order_acceptance_id": order_id,
        }
        res = await self._request("POST", "/v1/me/cancelchildorder", body=body)
        if return_raw_response:
            return res

        # bitFlyer returns empty response on successful cancel
        if res is None or res == {} or (isinstance(res, dict) and "success" not in res):
            # Success
            if order_id in self._order_cache:
                oinfo = copy(self._order_cache[order_id])
                oinfo.timestamp = datetime.datetime.now(datetime.timezone.utc)
                oinfo.market_created_timestamp = datetime.datetime.now(datetime.timezone.utc)
                oinfo.order_status = OrderStatus.Canceled
                oinfo.src_type = src_type
                oinfo.src_id = src_id
                oinfo.src_timestamp = src_timestamp
                oinfo.misc = misc
                oinfo.universal_id = uuid4().hex
                self._order_cache[oinfo.order_id] = oinfo
                asyncio.create_task(self.ticker_plant.info(json.dumps(oinfo.to_dict())))  # store
                return oinfo
            else:
                # Create minimal order object
                oinfo = Order(
                    timestamp=datetime.datetime.now(datetime.timezone.utc),
                    market_created_timestamp=datetime.datetime.now(datetime.timezone.utc),
                    sym=self.as_common_symbol(product_code),
                    side=0,
                    price=0,
                    amount=0,
                    executed_amount=0,
                    order_type="",
                    order_status=OrderStatus.Canceled,
                    venue=VENUE.BITFLYER,
                    order_id=order_id,
                    src_type=src_type,
                    src_id=src_id,
                    src_timestamp=src_timestamp,
                    misc=misc,
                )
                return oinfo
        else:
            self.logger.warning(f"APIError: {res}")
            if order_id in self._order_cache:
                return copy(self._order_cache[order_id])
            return None

    async def cancel_all_orders(
        self,
        timestamp: datetime.datetime,
        sym: str,
        own_only=False
    ):
        """Cancel all open orders for a symbol."""
        self.logger.info(f"Try canceling all open orders (sym={sym})...")

        if not own_only:
            # Use cancelallchildorders API
            body = {"product_code": self.as_bf_symbol(sym)}
            res = await self._request("POST", "/v1/me/cancelallchildorders", body=body)
            self.logger.info(f"cancelallchildorders result: {res}")
            # Mark all cached orders as canceled
            for order_id, order in list(self._order_cache.items()):
                if order.sym == sym and order.order_status in (
                    OrderStatus.Sent, OrderStatus.New, OrderStatus.Partial
                ):
                    order.order_status = OrderStatus.Canceled
                    order.timestamp = datetime.datetime.now(datetime.timezone.utc)
                    self._order_cache[order_id] = order
            return []

        # Cancel only own orders
        res = await self.fetch_open_orders(sym)
        if not isinstance(res, list):
            self.logger.error(f"Failed to fetch open orders: {res}")
            return []

        results = []
        for o in res:
            acceptance_id = o.get("child_order_acceptance_id")
            if not own_only or acceptance_id in self._order_cache:
                self.logger.info(f"Cancel order (order_id = {acceptance_id})")
                try:
                    order = await self.cancel_order(
                        order_id=acceptance_id,
                        timestamp=datetime.datetime.now(datetime.timezone.utc),
                        src_type=None,
                        src_timestamp=datetime.datetime.now(datetime.timezone.utc),
                        sym=sym,
                    )
                    results.append(order)
                except Exception as e:
                    self.logger.error(f"Failed to cancel order {acceptance_id}: {e}")
        self.logger.info(f"Canceled orders: {results}")
        return results

    async def check_order(
        self,
        order_id: str,
        sym: str = None,
        return_raw_response=False
    ) -> Union[Dict, Order]:
        """Check order status."""
        if sym is None:
            if order_id in self._order_cache:
                sym = self._order_cache[order_id].sym
            else:
                raise ValueError("sym must be provided if order_id is not in cache")

        params = {
            "product_code": self.as_bf_symbol(sym),
            "child_order_acceptance_id": order_id,
        }
        res = await self._request("GET", "/v1/me/getchildorders", params=params)
        if return_raw_response:
            return res

        if isinstance(res, list) and len(res) > 0:
            order_data = res[0]
            if order_id in self._order_cache:
                data = copy(self._order_cache[order_id])
            else:
                data = Order(
                    timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
                    market_created_timestamp=pd.NaT,
                    sym=sym,
                    side=1 if order_data["side"] == "BUY" else -1,
                    price=order_data.get("price", 0),
                    amount=order_data["size"],
                    executed_amount=order_data["executed_size"],
                    order_type=OrderType.Limit if order_data["child_order_type"] == "LIMIT" else OrderType.Market,
                    order_status=self._convert_order_status(order_data["child_order_state"]),
                    venue=VENUE.BITFLYER,
                    order_id=order_id,
                )

            data.timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
            data.market_created_timestamp = pd.to_datetime(order_data["child_order_date"]).tz_localize("UTC")
            data.executed_amount = order_data["executed_size"]
            data.amount = order_data["size"]
            data.order_status = self._convert_order_status(order_data["child_order_state"])
            if order_data.get("average_price"):
                data.price = order_data["average_price"]
            data.universal_id = uuid4().hex
            self._order_cache[order_id] = data
            asyncio.create_task(self.ticker_plant.info(json.dumps(data.to_dict())))
            return data

        return None

    async def amend_order(self, *args, **kwargs):
        """Amend order - not supported by bitFlyer."""
        raise NotImplementedError("bitFlyer does not support order amendment")

    async def fetch_open_orders(self, sym: str) -> List[Dict]:
        """Fetch active orders."""
        params = {
            "product_code": self.as_bf_symbol(sym),
            "child_order_state": "ACTIVE",
        }
        res = await self._request("GET", "/v1/me/getchildorders", params=params)
        return res

    async def fetch_transactions(
        self,
        sym: str,
        count: int = 100,
        before: int = None,
        after: int = None,
    ) -> List[Dict]:
        """Fetch execution history."""
        params = {"product_code": self.as_bf_symbol(sym), "count": count}
        if before is not None:
            params["before"] = before
        if after is not None:
            params["after"] = after
        res = await self._request("GET", "/v1/me/getexecutions", params=params)
        return res

    async def fetch_open_positions(self, sym: str = "FXBTCJPY") -> List[Dict]:
        """Fetch open positions (CFD only)."""
        params = {"product_code": self.as_bf_symbol(sym)}
        res = await self._request("GET", "/v1/me/getpositions", params=params)
        if isinstance(res, list):
            self.logger.info(f"Fetched positions: {res}")
            return res
        else:
            self.logger.error(f"Failed to fetch positions: {res}")
            return []

    async def fetch_account_balance(self) -> List[Dict]:
        """Fetch account balance."""
        res = await self._request("GET", "/v1/me/getbalance")
        return res

    async def fetch_collateral(self) -> Dict:
        """Fetch collateral information."""
        res = await self._request("GET", "/v1/me/getcollateral")
        return res

    def _convert_order_status(self, status: str) -> OrderStatus:
        """Convert bitFlyer order status to internal status."""
        mapping = {
            "ACTIVE": OrderStatus.Sent,
            "COMPLETED": OrderStatus.Filled,
            "CANCELED": OrderStatus.Canceled,
            "EXPIRED": OrderStatus.Canceled,
            "REJECTED": OrderStatus.Canceled,
        }
        return mapping.get(status.upper(), OrderStatus.Sent)

    async def on_message(self, msg: Dict):
        """Handle incoming messages from streaming."""
        if "_data_type" not in msg:
            return
        dtype = msg.get("_data_type")

        if dtype in ("Order", "Trade") and msg.get("venue") == VENUE.BITFLYER:
            self._last_stream_msg_timestamp = datetime.datetime.now()

        if dtype == "Order":
            msg.pop("_data_type")
            oinfo = Order(**msg)
            if oinfo.order_id in self._order_cache:
                oinfo_old = self._order_cache[oinfo.order_id]
                oinfo.model_id = oinfo_old.model_id
                oinfo.process_id = oinfo_old.process_id
                oinfo.src_type = "Order"
                oinfo.src_id = oinfo.universal_id
                oinfo.src_timestamp = oinfo.timestamp
                oinfo.timestamp = datetime.datetime.now(datetime.timezone.utc)
                self._order_cache[oinfo.order_id] = copy(oinfo)
                self.logger.info(f"Order status updated, order_id = {oinfo.order_id}, status = {oinfo.order_status}")
                await self.strategy.on_message(oinfo.to_dict(to_string_timestamp=False))
                asyncio.create_task(self.ticker_plant.info(json.dumps(oinfo.to_dict())))
            else:
                self.logger.info(f"No cache found, skip order update message for order_id = {oinfo.order_id}")

        if dtype == "Trade":
            msg.pop("_data_type")
            trade = Trade(**msg)
            if trade.order_id in self._order_cache and trade.trade_id not in self._transaction_cache:
                oinfo = self._order_cache[trade.order_id]
                trade.timestamp = datetime.datetime.now(datetime.timezone.utc)
                trade.model_id = oinfo.model_id
                trade.process_id = oinfo.process_id
                trade.universal_id = uuid4().hex
                self._transaction_cache[trade.trade_id] = trade
                self.update_position(trade=trade)
                await self.strategy.on_message(trade.to_dict(to_string_timestamp=False))
                asyncio.create_task(self.ticker_plant.info(json.dumps(trade.to_dict())))
                self.logger.info(f"Found new transaction: \n{trade.to_dict()}")
                self.logger.info(f"Current position = {self.positions}")

        # Pop older cache
        t_theta = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=14)
        if len(self._order_cache) > 10000:
            self._order_cache = {k: o for k, o in self._order_cache.items() if t_theta < o.timestamp}
        if len(self._transaction_cache) > 10000:
            self._transaction_cache = {k: t for k, t in self._transaction_cache.items() if t_theta < t.timestamp}
