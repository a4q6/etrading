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


class GmoRestClient(ExchangeClientBase):
    """
    GMO Coin Private REST API Client
    https://api.coin.z.com/docs/

    Supports both spot (BTC, ETH, etc.) and leverage (BTC_JPY, ETH_JPY, etc.) trading.
    """

    BASE_URL = "https://api.coin.z.com/private"

    def as_gmo_symbol(self, sym: str) -> str:
        """
        Convert internal symbol to GMO format.
        BTCJPY -> BTC_JPY (leverage), BTC -> BTC (spot)
        """
        if sym.endswith("JPY") and len(sym) > 3:
            # BTCJPY -> BTC_JPY (leverage)
            return sym[:-3] + "_" + sym[-3:]
        else:
            # BTC -> BTC (spot)
            return sym

    def as_common_symbol(self, sym: str) -> str:
        """
        Convert GMO symbol to internal format.
        BTC_JPY -> BTCJPY, BTC -> BTC
        """
        return sym.replace("_", "").upper()

    def __init__(
        self,
        api_key: str = Config.GMO_API_KEY,
        api_secret: str = Config.GMO_API_SECRET,
        log_file: Optional[str] = None,
        tp_number: int = 1,
        **kwargs,
    ):
        self.api_key = api_key
        self.api_secret = api_secret

        # logger
        tp_file = Path(Config.TP_DIR)
        self.ticker_plant = AsyncBufferedLogger(
            logger_name=f"TP-GmoPrivate{tp_number}-ALL",
            log_dir=tp_file.as_posix()
        )
        logger_name = "gmo-client" if log_file is None else log_file.split("/")[-1].split(".")[0]
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

        # Position cache for leverage trading
        # {sym: {"BUY": [{positionId, size, price}, ...], "SELL": [...]}}
        self._position_cache: Dict[str, Dict[str, List[Dict]]] = {}
        self._position_cache_timestamp: Dict[str, datetime.datetime] = {}

    def _generate_signature(self, method: str, path: str, body: str = ""):
        """Generate authentication headers for GMO API."""
        timestamp = str(int(time.time() * 1000))
        # signature = HMAC-SHA256(timestamp + method + path + body, secret)
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
            # "Content-Type": "application/json"
        }
        return headers

    async def _request(
        self,
        method: str,
        path: str,
        params: dict = None,
        body: dict = None,
    ) -> Dict:
        """Make authenticated request to GMO API."""
        url = self.BASE_URL + path

        # Build query string for GET
        if method.upper() == "GET" and params:
            url = self.BASE_URL + path
            body_str = ""
        else:
            body_str = json.dumps(body) if body else ""

        headers = self._generate_signature(method=method.upper(), path=path, body=body_str)

        async with aiohttp.ClientSession() as session:
            if method.upper() == "GET":
                async with session.get(url, headers=headers, params=params) as resp:
                    result = await resp.json()
                    if result.get("status") != 0:
                        self.logger.error(f"API Error: {result}")
                    return result
            elif method.upper() == "POST":
                async with session.post(url, headers=headers, data=body_str) as resp:
                    result = await resp.json()
                    if result.get("status") != 0:
                        self.logger.error(f"API Error: {result}")
                    return result
            else:
                raise NotImplementedError(f"HTTP method {method} not supported")

    def _is_leverage_symbol(self, sym: str) -> bool:
        """Check if symbol is for leverage trading (XXX_JPY format)."""
        return sym.endswith("JPY") and len(sym) > 3

    async def _get_opposite_position_size(self, sym: str, side: int) -> float:
        """
        Get total size of positions on the opposite side.

        Args:
            sym: Symbol (internal format, e.g., BTCJPY)
            side: Order side (+1 for BUY, -1 for SELL)

        Returns:
            Total size of opposite side positions
        """
        # Check cache validity (5 seconds)
        cache_valid = (
            sym in self._position_cache_timestamp and
            datetime.datetime.now() - self._position_cache_timestamp.get(sym, datetime.datetime.min) < datetime.timedelta(seconds=5)
        )

        if not cache_valid:
            # Fetch fresh positions
            positions = await self.fetch_open_positions(sym)
            self._position_cache[sym] = {"BUY": 0.0, "SELL": 0.0}
            for pos in positions:
                pos_side = pos.get("side")
                self._position_cache[sym][pos_side] += float(pos.get("size", 0))
            self._position_cache_timestamp[sym] = datetime.datetime.now()

        # Return opposite side total size
        opposite_side = "SELL" if side > 0 else "BUY"
        return self._position_cache.get(sym, {}).get(opposite_side, 0.0)

    async def send_order(
        self,
        timestamp: datetime.datetime,
        sym: str,  # BTCJPY, BTC, etc.
        side: int,  # -1/+1
        price: Optional[float],
        amount: float,
        order_type: OrderType,
        src_type: str,
        src_timestamp: datetime.datetime,
        src_id=None,
        return_raw_response=False,
        misc=None,
        time_in_force: str = None,  # FAK, FAS, FOK, SOK, (Post-Only for spot)
        losscut_price: float = None,
        auto_close: bool = True,  # Auto-use closeOrder for opposite positions
        **kwargs
    ):
        """
        Send new order to GMO.

        For leverage symbols (XXX_JPY), if auto_close=True and there are
        existing positions on the opposite side, uses /v1/closeOrder instead
        of /v1/order to close positions.

        executionType:
        - MARKET: Market order
        - LIMIT: Limit order
        - STOP: Stop order

        timeInForce:
        - FAK: Fill and Kill
        - FAS: Fill and Store
        - FOK: Fill or Kill
        - SOK: Store or Kill (Post-Only for leverage)

        auto_close:
        - True: Automatically use closeOrder for opposite positions (default)
        - False: Always use regular order
        """
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

        # Check if we should use closeBulkOrder for leverage positions
        use_close_order = False
        close_size = 0.0
        remaining_amount = amount

        if auto_close and self._is_leverage_symbol(sym):
            opposite_size = await self._get_opposite_position_size(sym, side)
            if opposite_size > 0:
                # Calculate how much we can close
                close_size = min(opposite_size, amount)
                remaining_amount = amount - close_size
                use_close_order = True
                self.logger.info(
                    f"Using closeBulkOrder to settle {close_size}, "
                    f"remaining new order: {remaining_amount}"
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
            venue=VENUE.GMO,
            model_id=self.strategy.model_id if self.strategy else None,
            process_id=self.strategy.process_id if self.strategy else None,
            src_type=src_type,
            src_id=src_id,
            src_timestamp=src_timestamp,
            misc=misc,
        )
        asyncio.create_task(self.ticker_plant.info(json.dumps(data.to_dict())))  # store (new)

        # Send close bulk order if needed
        if use_close_order:
            close_result = await self._send_close_bulk_order(
                sym=sym,
                side=side,
                price=price,
                size=close_size,
                order_type=order_type,
                time_in_force=time_in_force,
            )

            if return_raw_response:
                return close_result

            # If all amount was covered by close order, return
            if remaining_amount <= 0:
                if close_result.get("status") == 0:
                    order_id = close_result.get("data")
                    data.timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
                    data.order_id = str(order_id)
                    data.order_status = OrderStatus.Sent
                    data.universal_id = uuid4().hex
                    data.misc = "closeBulkOrder"
                    self._order_cache[data.order_id] = data
                    asyncio.create_task(self.ticker_plant.info(json.dumps(data.to_dict())))
                    self.logger.info(
                        f"Sent closeBulkOrder (order_id, type, side, price, size) = "
                        f"({data.order_id}, {data.order_type}, {data.side}, {data.price}, {data.amount})"
                    )
                    self._last_order_timestamp = datetime.datetime.now()
                    # Invalidate position cache
                    self._position_cache_timestamp.pop(sym, None)
                    return data
                else:
                    self.logger.error(f"Failed to send closeBulkOrder: {close_result}")
                    data.order_status = OrderStatus.Canceled
                    data.misc = str(close_result.get("messages", close_result))
                    asyncio.create_task(self.ticker_plant.info(json.dumps(data.to_dict())))
                    self._api_error_count.increment()
                    return data

            # If partial close, continue with remaining amount as new order
            amount = remaining_amount
            data.amount = remaining_amount

        # Build request body for regular order
        body = {
            "symbol": self.as_gmo_symbol(sym),
            "side": "BUY" if side > 0 else "SELL",
            "size": str(amount),
        }

        if order_type == OrderType.Limit:
            body["executionType"] = "LIMIT"
            body["price"] = str(int(price))
            body["timeInForce"] = time_in_force
        elif order_type == OrderType.Market:
            body["executionType"] = "MARKET"
        elif order_type == OrderType.Stop:
            body["executionType"] = "STOP"
            body["price"] = str(int(price))
        else:
            raise ValueError(f"Unsupported order type: {order_type}")

        if losscut_price is not None:
            body["losscutPrice"] = str(int(losscut_price))

        # Send request
        res = await self._request("POST", "/v1/order", body=body)
        if return_raw_response:
            return res

        if res.get("status") != 0:
            self.logger.error(f"Failed to send new order (UUID = {data.universal_id})\n{res}")
            data.order_status = OrderStatus.Canceled
            data.misc = str(res.get("messages", res))
            asyncio.create_task(self.ticker_plant.info(json.dumps(data.to_dict())))  # store (canceled)
            self._api_error_count.increment()
            return data

        # Update order info
        order_id = res.get("data")  # GMO returns order ID directly in "data" field
        data.timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        data.order_id = str(order_id)
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

    async def _send_close_bulk_order(
        self,
        sym: str,
        side: int,
        price: Optional[float],
        size: float,
        order_type: OrderType,
        time_in_force: str = None,
    ) -> Dict:
        """
        Send close bulk order to settle existing positions.

        Uses /v1/closeBulkOrder API which settles positions by symbol/side/size
        without specifying individual position IDs.

        Args:
            sym: Symbol (internal format)
            side: Order side (+1 for BUY, -1 for SELL)
            price: Order price (required for LIMIT)
            size: Total size to close
            order_type: Order type (MARKET or LIMIT)
            time_in_force: Time in force

        Returns:
            API response
        """
        body = {
            "symbol": self.as_gmo_symbol(sym),
            "side": "BUY" if side > 0 else "SELL",
            "size": str(size),
        }

        if order_type == OrderType.Limit:
            body["executionType"] = "LIMIT"
            body["price"] = str(int(price))
            if time_in_force:
                body["timeInForce"] = time_in_force
        elif order_type == OrderType.Market:
            body["executionType"] = "MARKET"
        else:
            raise ValueError(f"Unsupported order type for closeBulkOrder: {order_type}")

        self.logger.info(f"Sending closeBulkOrder: {body}")
        res = await self._request("POST", "/v1/closeBulkOrder", body=body)
        return res

    async def send_close_order(
        self,
        timestamp: datetime.datetime,
        sym: str,
        side: int,
        price: Optional[float],
        amount: float,
        order_type: OrderType,
        src_type: str,
        src_timestamp: datetime.datetime,
        src_id=None,
        return_raw_response=False,
        misc=None,
        time_in_force: str = None,
        **kwargs
    ):
        """
        Explicitly send close bulk order.

        Uses /v1/closeBulkOrder to close positions by size.
        """
        # Check if there are positions to close
        opposite_size = await self._get_opposite_position_size(sym, side)
        if opposite_size <= 0:
            raise ValueError(f"No opposite positions found to close for {sym}")

        close_size = min(opposite_size, amount)

        # Create order info
        data = Order(
            datetime.datetime.now(datetime.timezone.utc),
            market_created_timestamp=pd.NaT,
            sym=sym,
            side=side,
            price=price,
            amount=close_size,
            executed_amount=0,
            order_type=order_type,
            order_status=OrderStatus.New,
            venue=VENUE.GMO,
            model_id=self.strategy.model_id if self.strategy else None,
            process_id=self.strategy.process_id if self.strategy else None,
            src_type=src_type,
            src_id=src_id,
            src_timestamp=src_timestamp,
            misc=misc or "closeBulkOrder",
        )
        asyncio.create_task(self.ticker_plant.info(json.dumps(data.to_dict())))

        res = await self._send_close_bulk_order(
            sym=sym,
            side=side,
            price=price,
            size=close_size,
            order_type=order_type,
            time_in_force=time_in_force,
        )

        if return_raw_response:
            return res

        if res.get("status") == 0:
            order_id = res.get("data")
            data.timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
            data.order_id = str(order_id)
            data.order_status = OrderStatus.Sent
            data.universal_id = uuid4().hex
            self._order_cache[data.order_id] = data
            asyncio.create_task(self.ticker_plant.info(json.dumps(data.to_dict())))
            self.logger.info(
                f"Sent closeBulkOrder (order_id, type, side, price, size) = "
                f"({data.order_id}, {data.order_type}, {data.side}, {data.price}, {data.amount})"
            )
            self._last_order_timestamp = datetime.datetime.now()
            # Invalidate position cache
            self._position_cache_timestamp.pop(sym, None)
            return data
        else:
            self.logger.error(f"Failed to send closeBulkOrder: {res}")
            data.order_status = OrderStatus.Canceled
            data.misc = str(res.get("messages", res))
            asyncio.create_task(self.ticker_plant.info(json.dumps(data.to_dict())))
            self._api_error_count.increment()
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
        body = {"orderId": int(order_id)}
        res = await self._request("POST", "/v1/cancelOrder", body=body)
        if return_raw_response:
            return res

        if res.get("status") == 0:
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
                asyncio.create_task(self.ticker_plant.info(json.dumps(oinfo.to_dict())))
                return oinfo
            else:
                oinfo = Order(
                    timestamp=datetime.datetime.now(datetime.timezone.utc),
                    market_created_timestamp=datetime.datetime.now(datetime.timezone.utc),
                    sym=sym or "",
                    side=0,
                    price=0,
                    amount=0,
                    executed_amount=0,
                    order_type="",
                    order_status=OrderStatus.Canceled,
                    venue=VENUE.GMO,
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
        own_only=False,
        side: int = None,
    ):
        """Cancel all open orders for a symbol."""
        self.logger.info(f"Try canceling all open orders (sym={sym})...")

        # Use cancelBulkOrder API
        body = {"symbols": [self.as_gmo_symbol(sym)]}
        if side is not None:
            body["side"] = "BUY" if side > 0 else "SELL"
        res = await self._request("POST", "/v1/cancelBulkOrder", body=body)
        self.logger.info(f"cancelBulkOrder result: {res}")

        # Mark all cached orders as canceled
        for order_id, order in list(self._order_cache.items()):
            if order.sym == sym and order.order_status in (
                OrderStatus.Sent, OrderStatus.New, OrderStatus.Partial
            ):
                if side is None or order.side == side:
                    order.order_status = OrderStatus.Canceled
                    order.timestamp = datetime.datetime.now(datetime.timezone.utc)
                    self._order_cache[order_id] = order

        return []

    async def check_order(
        self,
        order_id: str,
        sym: str = None,
        return_raw_response=False
    ) -> Union[Dict, Order]:
        """Check order status."""
        params = {"orderId": order_id}
        res = await self._request("GET", "/v1/orders", params=params)
        if return_raw_response:
            return res

        if res.get("status") == 0 and res.get("data") and len(res["data"]["list"]) > 0:
            order_data = res["data"]["list"][0]
            if order_id in self._order_cache:
                data = copy(self._order_cache[order_id])
            else:
                data = Order(
                    timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
                    market_created_timestamp=pd.NaT,
                    sym=self.as_common_symbol(order_data["symbol"]),
                    side=1 if order_data["side"] == "BUY" else -1,
                    price=float(order_data.get("price", 0)) if order_data.get("price") else 0,
                    amount=float(order_data["size"]),
                    executed_amount=float(order_data.get("executedSize", 0)),
                    order_type=self._convert_execution_type(order_data["executionType"]),
                    order_status=self._convert_order_status(order_data["status"]),
                    venue=VENUE.GMO,
                    order_id=order_id,
                )

            data.timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
            if order_data.get("timestamp"):
                data.market_created_timestamp = pd.to_datetime(order_data["timestamp"]).tz_convert("UTC")
            data.executed_amount = float(order_data.get("executedSize", 0))
            data.amount = float(order_data["size"])
            data.order_status = self._convert_order_status(order_data["status"])
            if order_data.get("averagePrice"):
                data.price = float(order_data["averagePrice"])
            data.universal_id = uuid4().hex
            self._order_cache[order_id] = data
            asyncio.create_task(self.ticker_plant.info(json.dumps(data.to_dict())))
            return data

        return None

    async def amend_order(
        self,
        order_id: str,
        timestamp: datetime.datetime,
        price: float = None,
        losscut_price: float = None,
        src_type: str = None,
        src_timestamp: datetime.datetime = None,
        src_id: str = None,
        return_raw_response=False,
        **kwargs
    ) -> Order:
        """Change order price."""
        body = {"orderId": int(order_id)}
        if price is not None:
            body["price"] = str(int(price))
        if losscut_price is not None:
            body["losscutPrice"] = str(int(losscut_price))

        res = await self._request("POST", "/v1/changeOrder", body=body)
        if return_raw_response:
            return res

        if res.get("status") == 0 and order_id in self._order_cache:
            oinfo = copy(self._order_cache[order_id])
            oinfo.timestamp = datetime.datetime.now(datetime.timezone.utc)
            if price is not None:
                oinfo.price = price
            oinfo.order_status = OrderStatus.Updated
            oinfo.src_type = src_type
            oinfo.src_id = src_id
            oinfo.src_timestamp = src_timestamp
            oinfo.universal_id = uuid4().hex
            self._order_cache[order_id] = oinfo
            asyncio.create_task(self.ticker_plant.info(json.dumps(oinfo.to_dict())))
            return oinfo
        return None

    async def fetch_open_orders(self, sym: str) -> List[Dict]:
        """Fetch active orders."""
        params = {"symbol": self.as_gmo_symbol(sym)}
        res = await self._request("GET", "/v1/activeOrders", params=params)
        if res.get("status") == 0 and res.get("data"):
            return res["data"].get("list", [])
        return []

    async def fetch_execution(
        self,
        order_id: str = None,
        execution_id: str = None,
    ) -> List[Dict]:
        """Fetch execution history."""
        params = {}
        if order_id is not None:
            params["orderId"] = order_id
        if execution_id is not None:
            params["executionId"] = execution_id
        res = await self._request("GET", "/v1/executions", params=params)
        if res.get("status") == 0 and res.get("data"):
            return res["data"].get("list", [])
        return []

    async def fetch_transactions(
        self,
        sym: str,
        count: int = 100,
    ) -> List[Dict]:
        """Fetch latest executions."""
        params = {"symbol": self.as_gmo_symbol(sym), "count": count}
        res = await self._request("GET", "/v1/latestExecutions", params=params)
        if res.get("status") == 0 and res.get("data"):
            return res["data"].get("list", [])
        return []

    async def fetch_open_positions(self, sym: str) -> List[Dict]:
        """Fetch open positions (leverage only)."""
        params = {}
        if sym is not None:
            params["symbol"] = self.as_gmo_symbol(sym)
        res = await self._request("GET", "/v1/openPositions", params=params)
        if res.get("status") == 0 and res.get("data"):
            positions = res["data"].get("list", [])
            self.logger.info(f"Fetched positions: {positions}")
            return positions
        return []

    async def fetch_position_summary(self, sym: str = None) -> List[Dict]:
        """Fetch position summary (leverage only)."""
        params = {}
        if sym is not None:
            params["symbol"] = self.as_gmo_symbol(sym)
        res = await self._request("GET", "/v1/positionSummary", params=params)
        if res.get("status") == 0 and res.get("data"):
            return res["data"].get("list", [])
        return []

    async def fetch_account_margin(self) -> Dict:
        """Fetch account margin information."""
        res = await self._request("GET", "/v1/account/margin")
        if res.get("status") == 0 and res.get("data"):
            return res["data"]
        return {}

    async def fetch_account_assets(self) -> List[Dict]:
        """Fetch account assets (spot balances)."""
        res = await self._request("GET", "/v1/account/assets")
        if res.get("status") == 0 and res.get("data"):
            return res["data"]
        return []

    async def fetch_account_balance(self) -> Dict:
        """Fetch account balance (combines margin and assets)."""
        margin = await self.fetch_account_margin()
        assets = await self.fetch_account_assets()
        return {"margin": margin, "assets": assets}

    async def get_ws_access_token(self) -> str:
        """Get access token for Private WebSocket API."""
        res = await self._request("POST", "/v1/ws-auth", body={})
        if res.get("status") == 0 and res.get("data"):
            return res["data"]
        raise RuntimeError(f"Failed to get WebSocket access token: {res}")

    async def extend_ws_access_token(self, token: str) -> bool:
        """Extend access token for Private WebSocket API."""
        body = {"token": token}
        res = await self._request("PUT", "/v1/ws-auth", body=body)
        return res.get("status") == 0

    async def delete_ws_access_token(self, token: str) -> bool:
        """Delete access token for Private WebSocket API."""
        body = {"token": token}
        res = await self._request("DELETE", "/v1/ws-auth", body=body)
        return res.get("status") == 0

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

    async def on_message(self, msg: Dict):
        """Handle incoming messages from streaming."""
        if "_data_type" not in msg:
            return
        dtype = msg.get("_data_type")

        if dtype in ("Order", "Trade") and msg.get("venue") == VENUE.GMO:
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
