import hmac
import hashlib
import urllib.parse
import json
import aiohttp
import datetime
import time
import pandas as pd
import asyncio
import numpy as np
from pathlib import Path
from uuid import uuid4
from typing import Optional, Dict, Union
from copy import deepcopy
from etr.core.datamodel import VENUE, OrderType, OrderStatus, Order, Trade, Rate
from etr.core.api_client.base import ExchangeClientBase
from etr.strategy.base_strategy import StrategyBase
from etr.config import Config
from etr.core.async_logger import AsyncBufferedLogger


class CoincheckRestClient(ExchangeClientBase):

    def as_cc_symbol(self, sym: str) -> str:
        return sym[:-3].lower() + "_" + sym[-3:].lower()

    def __init__(
        self,
        api_key: str = Config.COINCHECK_API_KEY,
        api_secret: str = Config.COINCHECK_API_SECRET,
        log_file="strategy.log",
        tp_number: int = 0,
    ):
        super().__init__(log_file=log_file)
        # TP
        tp_file = Path(Config.TP_DIR)
        self.ticker_plant = AsyncBufferedLogger(
            logger_name=f"TP-CoincheckPrivate{tp_number}-ALL",
            log_dir=tp_file.as_posix()
        )
        # exchange specific
        self.uri = "https://coincheck.com"
        self.api_key = api_key
        self.api_secret = api_secret
        self.venue = VENUE.COINCHECK

        # order_id -> Order
        self._order_cache: Dict[str, Order] = {}
        self._transaction_cache: Dict[str, Trade] = {}

        self.closed_pnl = 0
        self.open_pnl = {}
        self.positions = {}  # {sym: (vwap, amount)}

    def _create_headers(self, url: str, body: Optional[str] = "") -> dict:
        nonce = str(int(time.time() * 1000))
        message = nonce + url + (body or "")
        signature = hmac.new(self.api_secret.encode('utf-8'), message.encode('utf-8'), hashlib.sha256).hexdigest()

        return {
            "ACCESS-KEY": self.api_key,
            "ACCESS-NONCE": nonce,
            "ACCESS-SIGNATURE": signature,
            "Content-Type": "application/json"
        }

    async def send_order(
        self,
        timestamp: datetime.datetime,
        sym: str,  # BTCJPY,ETHJPY...etc.
        side: int,  # -1/+1
        price: Optional[float],
        amount: float,
        order_type: OrderType,
        src_type: str,
        src_timestamp: datetime.datetime,
        src_id = None, 
        **kwargs
    ):
        path = '/api/exchange/orders'
        url = self.uri + path
        if order_type == OrderType.Market:
            otype = "market_buy" if side > 0 else "market_sell"
        elif order_type == OrderType.Limit:
            otype = "buy" if side > 0 else "sell"
        else:
            raise ValueError(f"Unexpected `order_type` : {order_type}")

        # request header + body
        pair = self.as_cc_symbol(sym=sym)
        if otype == 'market_buy':
            buy_amount = amount * price
            price = np.nan
            body = {'pair': pair, 'order_type': otype, 'market_buy_amount': str(buy_amount)}  # Must specify JPY amount in case of MO-Buy
        else:
            body = {'pair': pair, 'order_type': otype, 'amount': str(amount)}
        if order_type != OrderType.Market:
            body['rate'] = price  # limit order
        body = json.dumps(body)
        headers = self._create_headers(url, body)

        # TP output
        order_info = Order(
            datetime.datetime.now(datetime.timezone.utc), market_created_timestamp=pd.NaT, sym=sym,
            side=side, price=price, amount=amount, executed_amount=0, order_type=order_type, order_status=OrderStatus.New,
            venue=self.venue, model_id=self.strategy.model_id, process_id=self.strategy.process_id, src_type=src_type, 
            src_id=src_id, src_timestamp=src_timestamp
        )
        asyncio.create_task(self.ticker_plant.info(json.dumps(order_info.to_dict())))  # store

        # Send request
        self.logger.info(f"Sending {order_type} order UID = {order_info.universal_id}")
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=body, headers=headers) as response:
                if response.status == 200:
                    res = await response.json()
                else:
                    msg = await response.text()
                    self.logger.warning(f"Error at sending order (status:{response.status}), {msg}")
                    return Order.null_order()

        # Response:
        # {"success": true, "id": 12345, "rate": "30010.0", "amount": "1.3", "order_type": "sell", "time_in_force": "good_til_cancelled", "stop_loss_rate": null, "pair": "btc_jpy", "created_at": "2015-01-10T05:55:38.000Z"}
        order_info.timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        order_info.market_created_timestamp = pd.Timestamp(res["created_at"])
        order_info.order_id = str(res["id"])
        order_info.order_status = OrderStatus.Sent
        order_info.universal_id = uuid4().hex
        self._order_cache[order_info.order_id] = order_info
        asyncio.create_task(self.ticker_plant.info(json.dumps(order_info.to_dict())))  # store (sent)

        # Check order in case of MO
        if order_type == OrderType.Market:
            await asyncio.sleep(0.5)
            await self.fetch_transactions()
            await self.check_order(order_id=order_info.order_id)

        return deepcopy(self._order_cache[order_info.order_id])

    async def cancel_order(
        self,
        order_id: str,
        timestamp,
        src_type,
        src_timestamp,
        src_id=None,
        misc=None,
        **kwargs
    ) -> Order:
        path = f"/api/exchange/orders/{order_id}"
        url = self.uri + path
        headers = self._create_headers(url)

        async with aiohttp.ClientSession() as session:
            async with session.delete(url, headers=headers) as response:
                if response.status == 200:
                    self.logger.info(f"order_id = {order_id} cancelled successfully")
                    data = await response.json()  # {"success": true, "id": 12345}
                    # write to TP
                    oinfo = self._order_cache[str(data["id"])]
                    oinfo.order_status = OrderStatus.Canceled
                    oinfo.timestamp = datetime.datetime.now(datetime.timezone.utc)
                    oinfo.src_type = src_type
                    oinfo.src_id = src_id
                    oinfo.src_timestamp = src_timestamp
                    oinfo.misc = misc
                    oinfo.universal_id = uuid4().hex
                    self._order_cache[oinfo.order_id] = oinfo
                    asyncio.create_task(self.ticker_plant.info(json.dumps(oinfo.to_dict())))  # store
                    return deepcopy(oinfo)
                else:
                    error = await response.text()
                    self.logger.warning(f"Failed to cancel order_id = {order_id}: {response.status} - {error}")

    async def amend_order(self, timestamp, order_id, price, amount, order_type, src_type, src_timestamp, src_id = None, **kwargs):
        raise NotImplementedError("Order amend is not supported at coincheck.")

    async def check_order(self, order_id: str, return_raw_response=False) -> Union[Dict, Order]:
        path = f"/api/exchange/orders/{order_id}"
        url = self.uri + path
        headers = self._create_headers(url)

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    self.logger.info(f"Retrieved order status of order_id = {order_id}")
                    data = await response.json()
                    if return_raw_response:
                        return data
                    # {
                    # "success": true,
                    # "id": 12345,
                    # "pair": "btc_jpy",
                    # "status": "PARTIALLY_FILLED_EXPIRED",
                    # "order_type": "buy",
                    # "rate": "0.1",
                    # "stop_loss_rate": null,
                    # "maker_fee_rate": "0.001",
                    # "taker_fee_rate": "0.001",
                    # "amount": "1.0",
                    # "market_buy_amount": null,
                    # "executed_amount": "0",
                    # "executed_market_buy_amount": null,
                    # "expired_type": "self_trade_prevention",
                    # "prevented_match_id": 123,
                    # "expired_amount": "1.0",
                    # "expired_market_buy_amount": null,
                    # "time_in_force": "good_til_cancelled",
                    # "created_at": "2015-01-10T05:55:38.000Z"
                    # }
                    # write to TP
                    oinfo = self._order_cache[str(data["id"])]
                    oinfo.timestamp = datetime.datetime.now(datetime.timezone.utc)
                    oinfo.market_created_timestamp = pd.Timestamp(data["created_at"])
                    if any(data["status"] == status for status in ("NEW", "WAITING_FOR_TRIGGER")):
                        oinfo.order_status = OrderStatus.New
                    elif data["status"] == "FILLED":
                        oinfo.order_status = OrderStatus.Filled
                    elif data["status"] == "PARTIALLY_FILLED":
                        oinfo.order_status  = OrderStatus.Partial 
                    elif any(status in data["status"] for status in ("EXPIRED", "CANCELED")):
                        oinfo.order_status = OrderStatus.Canceled
                    if data["order_type"] == "market_buy":
                        amt = data["executed_market_buy_amount"]
                        oinfo.misc = str({'executed_market_buy_amount': float(amt)})
                    else:
                        oinfo.executed_amount = float(data["executed_amount"])
                    oinfo.universal_id = uuid4().hex
                    asyncio.create_task(self.ticker_plant.info(json.dumps(oinfo.to_dict())))  # store order info
                    return deepcopy(oinfo)
                else:
                    error = await response.text()
                    self.logger.warning(f"Failed to check order_id = {order_id}: {response.status} - {error}")

    async def fetch_transactions(self, return_raw_response=False, logging=True):
        path = f"/api/exchange/orders/transactions"
        url = self.uri + path
        headers = self._create_headers(url)

        # pop older cache
        t_theta = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=14)
        if len(self._order_cache) > 10000:
            self._order_cache = {k: o for k, o in self._order_cache.items() if t_theta < o.timestamp}
        if len(self._transaction_cache) > 10000:
            self._transaction_cache = {k: t for k, t in self._transaction_cache.items() if t_theta < t.timestamp}

        async with aiohttp.ClientSession() as session:
            if logging:
                self.logger.info(f"Requesting latest transactions...")
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    if logging:
                        self.logger.info(f"Received transactions")
                    data = await response.json()
                    if return_raw_response:
                        return data
                    # {
                    #   "success": true,
                    #   "transactions": [
                    #     {
                    #       "id": 38,
                    #       "order_id": 49,
                    #       "created_at": "2015-11-18T07:02:21.000Z",
                    #       "funds": {
                    #         "btc": "0.1",
                    #         "jpy": "-4096.135"
                    #       },
                    #       "pair": "btc_jpy",
                    #       "rate": "40900.0",
                    #       "fee_currency": "JPY",
                    #       "fee": "6.135",
                    #       "liquidity": "T",
                    #       "side": "buy"
                    #     }, ...
                    for msg in data["transactions"]:
                        oid = str(msg["order_id"])
                        tid = str(msg["id"])
                        if tid in self._transaction_cache:
                            continue
                        if oid in self._order_cache:
                            oinfo = self._order_cache[oid]
                            base_ccy = oinfo.sym.replace("JPY", "").lower()
                            amt = abs(float(msg["funds"][base_ccy]))
                            misc = {'fee': msg['fee'], 'liquidity': msg["liquidity"]}
                            oinfo.executed_amount = min(oinfo.executed_amount + amt, oinfo.amount)
                            oinfo.universal_id = uuid4().hex
                            oinfo.order_status = OrderStatus.Updated
                            if oinfo.order_type == OrderType.Market:
                                oinfo.price = float(msg["rate"])
                            trade: Trade = Trade.from_order(
                                timestamp=datetime.datetime.now(datetime.timezone.utc),
                                trade_id=tid,
                                market_created_timestamp=pd.Timestamp(msg["created_at"]),
                                price=float(msg["rate"]), exec_amount=abs(float(msg["funds"][base_ccy])),
                                order=oinfo, misc=str(misc),
                            )
                            self._transaction_cache[tid] = deepcopy(trade)
                            self._order_cache[oid] = deepcopy(oinfo)
                            self.update_position(trade=trade)
                            await self.strategy.on_message(oinfo.to_dict(to_string_timestamp=False))  # invoke strategy
                            await self.strategy.on_message(trade.to_dict(to_string_timestamp=False))  # invoke strategy
                            asyncio.create_task(self.ticker_plant.info(json.dumps(trade.to_dict())))  # store in TP
                            asyncio.create_task(self.ticker_plant.info(json.dumps(oinfo.to_dict())))  # store in TP
                            self.logger.info(f"Found new transaction: \n{trade.to_dict()}")

                else:
                    error = await response.text()
                    self.logger.warning(f"Failed to fetch latest transactions: {response.status} \n{error}")
                    return error

    async def loop_fetch_transactions(self, update_interval=10):
        while True:
            await self.fetch_transactions(logging=False)
            now = pd.Timestamp.now(tz="UTC")
            next_time = now.ceil(f"{update_interval}s")
            sleep_duration = (next_time - now).total_seconds()
            sleep_duration = sleep_duration if sleep_duration < 1 else sleep_duration  # add buffer
            await asyncio.sleep(sleep_duration)

    async def fetch_open_orders(self) -> Dict:
        path = f"/api/exchange/orders/opens"
        url = self.uri + path
        headers = self._create_headers(url)

        async with aiohttp.ClientSession() as session:
            self.logger.info(f"Requesting open orders")
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    self.logger.info(f"Received open orders")
                    return data
                else:
                    error = await response.text()
                    self.logger.warning(f"Failed to fetch open orders: {response.status} \n{error}")
                    return error

    async def fetch_balance(self) -> Dict:
        path = f"/api/accounts/balance"
        url = self.uri + path
        headers = self._create_headers(url)

        async with aiohttp.ClientSession() as session:
            self.logger.info(f"Requesting account balance")
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    self.logger.info(f"Received account balance")
                    return data
                else:
                    error = await response.text()
                    self.logger.warning(f"Failed to fetch account balance: {response.status} \n{error}")
                    return error

    async def fetch_account_info(self) -> Dict:
        path = f"/api/accounts"
        url = self.uri + path
        headers = self._create_headers(url)

        async with aiohttp.ClientSession() as session:
            self.logger.info(f"Requesting account info")
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    self.logger.info(f"Received account info")
                    return data
                else:
                    error = await response.text()
                    self.logger.warning(f"Failed to fetch account info: {response.status} \n{error}")
                    return error
