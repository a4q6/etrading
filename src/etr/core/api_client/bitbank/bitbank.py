import hmac
import hashlib
import json
import aiohttp
import datetime
import time
import pandas as pd
import asyncio
from typing import Optional, Dict, Union, List, Tuple
from copy import copy
from pathlib import Path
from uuid import uuid4
import pytz
from urllib.parse import urlparse, urlencode

from etr.core.api_client.realtime_counter import RealtimeCounter
from etr.core.async_logger import AsyncBufferedLogger
from etr.core.datamodel import VENUE, OrderType, OrderStatus, Order, Trade, Rate
from etr.core.api_client.base import ExchangeClientBase
from etr.common.logger import LoggerFactory
from etr.strategy.base_strategy import StrategyBase
from etr.config import Config
from .error_codes import BITBANK_ERROR_CODES


class BitbankRestClient(ExchangeClientBase):
    """
        https://github.com/bitbankinc/bitbank-api-docs/blob/master/rest-api_JP.md
    """

    BASE_URL = "https://api.bitbank.cc"

    def as_bb_symbol(self, sym: str) -> str:
        return sym[:-3].lower() + "_" + sym[-3:].lower()

    def as_common_symbol(self, sym: str) -> str:
        return sym.replace("_", "").upper()

    def __init__(
        self,
        api_key: str = Config.BITBANK_API_KEY,
        api_secret: str = Config.BITBANK_API_SECRET,
        log_file: Optional[str] = None,
        tp_number: int = 1,
        **kwargs,
    ):
        self.api_key = api_key
        self.api_secret = api_secret.encode()

        # logger
        tp_file = Path(Config.TP_DIR)
        self.ticker_plant = AsyncBufferedLogger(logger_name=f"TP-BitBankPrivate{tp_number}-ALL", log_dir=tp_file.as_posix())
        logger_name = "bb-client" if log_file is None else log_file.split("/")[-1].split(".")[0]
        if log_file is not None:
            log_file = Path(Config.LOG_DIR).joinpath(log_file).as_posix()
        self.logger = LoggerFactory().get_logger(logger_name=logger_name, log_file=log_file)
        self.closed_pnl = 0
        self.open_pnl = {}
        self.positions = {}  # {sym: (vwap, amount)}
        self.pending_positions = False
        self._api_error_count = {code: RealtimeCounter(window_sec=10) for code in BITBANK_ERROR_CODES.keys()}

        self._order_cache: Dict[str, Order] = {}
        self._transaction_cache: Dict[str, Trade] = {}
        self.strategy: StrategyBase = None
        self._margin_positions: Dict[Tuple[str], float] = {"initialized": False}  # sym -> (long, short)
        self._last_stream_msg_timestamp = pd.NaT
        self._last_order_timestamp = pd.NaT


    def _generate_signature(self, method: str, path: str, body: dict = None):
        nonce = str(int(time.time() * 1000))
        # build message
        if method.upper() == "GET":
            message = nonce + path
        elif method.upper() == "POST":
            if body is None:
                body_str = ""  # POSTはnonce + JSONボディ文字列を署名対象に
            else:
                # bodyは辞書を想定し、JSON文字列化
                body_str = json.dumps(body)  # 余計な空白を削除したcompact形式
            message = nonce + body_str
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        # build signature
        signature = hmac.new(self.api_secret, message.encode(), hashlib.sha256).hexdigest()
        headers = {
            "ACCESS-KEY": self.api_key,
            "ACCESS-NONCE": nonce,
            "ACCESS-SIGNATURE": signature,
            "Content-Type": "application/json"
        }
        return headers

    async def _request(self, method: str, path: str, params: dict = None, body: dict = None) -> Dict:
        if method == "GET" and params is not None:
            path = path + "?" + urlencode(params)  # add parameter at the end of request path in case of (GET, params)
            body = params  # move parameters to body to be embbeded in signature
            params = {}
        url = self.BASE_URL + path
        headers = self._generate_signature(method=method, path=path, body=body)
        async with aiohttp.ClientSession() as session:
            if method.upper() == "GET":
                async with session.get(url, headers=headers, params=params) as resp:
                    resp.raise_for_status()
                    return await resp.json()
            elif method.upper() == "POST":
                async with session.post(url, headers=headers, json=body) as resp:
                    resp.raise_for_status()
                    return await resp.json()
            else:
                raise NotImplementedError()

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
        use_margin = True,
        return_raw_response=False,
        misc=None,
        **kwargs
    ):
        if self._last_stream_msg_timestamp + datetime.timedelta(seconds=180) < self._last_order_timestamp:
            self.logger.error(f"Latest stream message ({self._last_stream_msg_timestamp}) is too old. Streaming might be dead now.")
            raise RuntimeError(f"Latest stream message ({self._last_stream_msg_timestamp}) is too old. Streaming might be dead now.")

        # order info
        amount = round(amount, 6)
        data = Order(
            datetime.datetime.now(datetime.timezone.utc), market_created_timestamp=pd.NaT, sym=sym,
            side=side, price=price, amount=amount, executed_amount=0, order_type=order_type, order_status=OrderStatus.New,
            venue=VENUE.BITBANK, model_id=self.strategy.model_id, process_id=self.strategy.process_id, src_type=src_type, 
            src_id=src_id, src_timestamp=src_timestamp, misc=misc,
        )
        asyncio.create_task(self.ticker_plant.info(json.dumps(data.to_dict())))  # store (new)

        # build request body
        body = {
            "pair": self.as_bb_symbol(sym),
            "side": "buy" if side > 0 else "sell",
            "type": order_type,
            "amount": str(amount)
        }
        if order_type == OrderType.Stop:
            body["trigger_price"] = str(price)
        elif order_type == OrderType.Limit:
            body["price"] = str(price)
            body["post_only"] = True  # https://support.bitbank.cc/hc/ja/articles/900005145623--%E5%8F%96%E5%BC%95%E6%89%80-Post-Only%E3%81%A8%E3%81%AF%E3%81%AA%E3%82%93%E3%81%A7%E3%81%99%E3%81%8B
        if use_margin and sym in ("BTCJPY", "ETHJPY", "XRPJPY", "SOLJPY", "DOGEJPY"):
            if self._margin_positions.get("initialized") == False:
                await self.fetch_open_positions()
            long, short = self._margin_positions[(sym, "long")], self._margin_positions[(sym, "short")]
            # open order amountを加味しないといけない
            if side > 0:
                if round(amount, 6) <= round(short, 6):
                    body["position_side"] = "short"
                else:
                    body["position_side"] = "long"
            elif side < 0:
                if round(amount, 6) <= round(long, 6):
                    body["position_side"] = "long"
                else:
                    body["position_side"] = "short"

        # send request
        res = await self._request("POST", "/v1/user/spot/order", body=body)
        if return_raw_response:
            return res
        if res.get("success") != 1:
            self.logger.error(f"Failed to send new order (UUID = {data.universal_id})\n{res}")
            self.logger.error(f"Error cause: {self.get_error_cause(res)}")
            data.order_status = OrderStatus.Canceled
            data.misc = self.get_error_cause(res)
            asyncio.create_task(self.ticker_plant.info(json.dumps(data.to_dict())))  # store (canceled)
            
            # increment error count
            code = int(res.get("data").get("code"))
            self._api_error_count[code].increment()

            if self._api_error_count[50062].count > 30:
                self.logger.info("Too many API error (50062), stop processing")
                raise RuntimeError("Too many API error.")
            elif self._api_error_count[50062].count > 2:  # 建玉数量を上回っています x N
                # check order & posiitons
                self.logger.info("Consective API error (50062) detected, enter reconciliation process")
                if len(self._order_cache) > 0:
                    for sym in set([o.sym for o in self._order_cache.values()]):
                        await self.cancel_all_orders(timestamp=datetime.datetime.now(tz=datetime.timezone.utc), sym=sym, own_only=True)
                await self._reconcile()

            return data

        res = res["data"]

        # update order info
        data.timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        data.market_created_timestamp = pytz.timezone("UTC").localize(datetime.datetime.fromtimestamp(float(res["ordered_at"]) / 1000))
        data.order_id = str(res["order_id"])
        data.amount = float(res["remaining_amount"])
        data.executed_amount = float(res["executed_amount"])
        data.order_status = self._convert_order_status(res["status"])
        data.universal_id = uuid4().hex
        if (data.order_type in [OrderType.Market, OrderType.Stop]) and (data.order_status in [OrderStatus.Filled, OrderStatus.Partial]):
            data.price = float(res["average_price"])
        elif (data.order_type in [OrderType.Stop]) and (data.order_status in [OrderStatus.Sent, OrderStatus.Canceled]):
            data.price = float(res["trigger_price"])
        elif data.order_type == OrderType.Limit:
            data.price = float(res["price"])
        misc = str({
            "position_side": res.get("position_side"),
            "trigger_price": res.get("trigger_price"),
            "average_price": res.get("average_price"),
            "price": res.get("price"),
        })
        self._order_cache[data.order_id] = data
        asyncio.create_task(self.ticker_plant.info(json.dumps(data.to_dict())))  # store (sent)
        self.logger.info(f"Sent new order (order_id, type, side, price) = ({data.order_id}, {data.order_type}, {data.side}, {data.price})")
        self._last_order_timestamp = datetime.datetime.now()
        return data

    async def _reconcile(self):
        self.logger.info("Start reconciliation")
        self.pending_positions = True
        await self.fetch_open_positions()  # check current account's position
        symbols = set([o.sym for o in self._order_cache.values()])
        for sym in symbols:
            # transactions取得 => _order_cache にあるが、 _transaction_cache にないTradeについて messageを作成して on_messageに渡す
            res = await self.fetch_transactions(sym)
            transactions = res["data"]["trades"]
            missing_trs = [msg for msg in transactions if str(msg["order_id"]) in self._order_cache and str(msg["trade_id"]) not in self._transaction_cache]
            if len(missing_trs) > 0:
                self.logger.info(f"Found missing transactions: {missing_trs}")
                for trade_msg in missing_trs:
                    # {'trade_id': 1415995982, 'order_id': 47771570657, 'pair': 'btc_jpy', 'side': 'sell', 'type': 'limit', 'amount': '0.0002', 'price': '17654577', 'maker_taker': 'maker', 'position_side': 'long', 'fee_amount_base': '0.00000000', 'fee_amount_quote': '-1.4129', 'fee_occurred_amount_quote': '-0.7062', 'profit_loss': '-1.25220082', 'interest': '0', 'executed_at': 1752702730478}
                    trade = Trade(
                        timestamp=datetime.datetime.now(datetime.timezone.utc),
                        market_created_timestamp=datetime.datetime.fromtimestamp(
                            trade_msg["executed_at"] / 1000, tz=datetime.timezone.utc
                        ),
                        sym=self.as_common_symbol(trade_msg["pair"]),
                        venue=VENUE.BITBANK,
                        side=1 if trade_msg["side"] == "buy" else -1,
                        price=float(trade_msg["price"]),
                        amount=float(trade_msg["amount"]),
                        order_id=str(trade_msg["order_id"]),
                        order_type=trade_msg["type"],  # "limit" or "market" or "stop"
                        trade_id=str(trade_msg["trade_id"]),
                        model_id=self.strategy.model_id,
                        process_id=self.strategy.process_id,
                        misc=str({'MT': trade_msg['maker_taker'], 'channel': 'REST'}),
                    )
                    asyncio.create_task(self.ticker_plant.info(json.dumps(trade.to_dict())))
                    await self.on_message(trade.to_dict(to_string_timestamp=False))

        self.pending_positions = False
        self.logger.info("Reconciliation process done")

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
        # async def cancel_order(self, pair: str, order_id: int):

        # build body
        if sym is not None:
            body = {"pair": self.as_bb_symbol(sym), "order_id": int(order_id)}
        else:
            oinfo = self._order_cache[order_id]
            body = {"pair": self.as_bb_symbol(oinfo.sym), "order_id": int(order_id)}
        res = await self._request("POST", "/v1/user/spot/cancel_order", body=body)
        if return_raw_response:
            return res

        if res.get("success") == 1:
            res = res["data"]
            oinfo = copy(self._order_cache[order_id])
            oinfo.timestamp = datetime.datetime.now(datetime.timezone.utc)
            oinfo.market_created_timestamp = datetime.datetime.fromtimestamp(float(res["canceled_at"]) / 1000).replace(tzinfo=pytz.timezone("UTC"))
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
            self.logger.warning(f"APIError: {self.get_error_cause(res)}")
            oinfo = self._order_cache[order_id]
            # increment error count
            code = int(res.get("data").get("code"))
            self._api_error_count[code].increment()

            if code in [50026, 70019]:
                oinfo.order_status = OrderStatus.Canceled
                self._order_cache[order_id] = oinfo
                self.logger.info("Update open positions via REST API")
                await self.fetch_open_positions()

            elif code in [50027]:
                oinfo.order_status = OrderStatus.Filled
                self._order_cache[order_id] = oinfo
                self.logger.info("Update open positions via REST API")
                await self.fetch_open_positions()

            self._order_cache[order_id] = oinfo
            return copy(oinfo)

    async def cancel_all_orders(self, timestamp: datetime.datetime, sym: str, own_only=False):
        self.logger.info(f"Try canceling all open orders (sym={sym})...")
        res = await self.fetch_open_orders(sym)
        if res.get('success') != 1:
            msg = self.get_error_cause(res)
            self.logger.error(f"Failed to fetch open orders: ({msg})", exc_info=True)
            raise RuntimeError(f"Failed to fetch open orders: ({msg})")

        orders = res["data"]["orders"]
        results = []
        for o in orders:
            if not own_only or str(o["order_id"]) in self._order_cache:
                self.logger.info(f"Cancel order (order_id = {o['order_id']})")
                try:
                    order = await self.cancel_order(
                        order_id=str(o["order_id"]),
                        timestamp=datetime.datetime.now(datetime.timezone.utc),
                        src_type=None,
                        src_timestamp=datetime.datetime.now(datetime.timezone.utc),
                    )
                    results.append(order)
                except:
                    pass
        self.logger.info(f"Canceled orders:\n{results}")
        return results

    @staticmethod
    def get_error_cause(response: dict) -> str:
        code = response.get("data").get("code")
        return f"{BITBANK_ERROR_CODES.get(code)}({code})"

    async def check_order(self, order_id: str, sym: str = None, return_raw_response=False) -> Union[Dict, Order]:
        if sym is None:
            data = self._order_cache[order_id]
            sym = data.sym
        params = {"pair": self.as_bb_symbol(sym), "order_id": int(order_id)}
        res = await self._request("GET", "/v1/user/spot/order", params=params)
        if return_raw_response:
            return res

        data.timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        data.market_created_timestamp = datetime.datetime.fromtimestamp(float(res["ordered_at"]) / 1000).replace(tzinfo=pytz.timezone("UTC"))
        data.order_id = str(res["order_id"])
        data.amount = float(res["remaining_amount"])
        data.executed_amount = float(res["executed_amount"])
        data.order_status = self._convert_order_status(res["status"])
        data.universal_id = uuid4().hex
        if (data.order_type in [OrderType.Market, OrderType.Stop]) and (data.order_status in [OrderStatus.Filled, OrderStatus.Partial]):
            data.price = float(res["average_price"])
        elif (data.order_type in [OrderType.Stop]) and (data.order_status in [OrderStatus.Sent, OrderStatus.Canceled]):
            data.price = float(res["trigger_price"])
        elif data.order_type == OrderType.Limit:
            data.price = float(res["price"])
        misc = str({
            "position_side": res.get("position_side"),
            "trigger_price": res.get("trigger_price"),
            "average_price": res.get("average_price"),
            "price": res.get("price"),
        })
        self._order_cache[data.order_id] = data
        asyncio.create_task(self.ticker_plant.info(json.dumps(data.to_dict())))  # store (sent)

    async def amend_order(*args, **kwargs):
        raise NotImplementedError()

    async def fetch_open_orders(self, sym: str) -> List[Dict]:
        params = {"pair": self.as_bb_symbol(sym)}
        res = await self._request("GET", path="/v1/user/spot/active_orders", params=params)
        if res.get("success") != 1:
            self.logger.warning(f"APIError: {self.get_error_cause(res)}")
        return res

    async def fetch_transactions(
        self,
        sym: str,
        count: int = 100,
        order_id: int = None,
        since: Optional[datetime.datetime] = None,
        end:  Optional[datetime.datetime] = None,
    ) -> List[Dict]:
        params = {"pair": self.as_bb_symbol(sym), "count": count}
        if order_id is not None:
            params["order_id"] = int(order_id)
        if since is not None:
            params["since"] =  int(since.timestamp() * 1000)  # unixtime[ms]
        if since is not None:
            params["end"] =  int(end.timestamp() * 1000)  # unixtime[ms]
        res = await self._request("GET", "/v1/user/spot/trade_history", params=params)
        return res

    async def fetch_open_positions(self) -> Dict:
        res = await self._request("GET", path="/v1/user/margin/positions")
        if res.get("success") != 1:
            self.logger.error(f"Failed to fetch open positions.")
            self.logger.warning(f"Error cause: {self.get_error_cause(res)}")
            return res
        else:
            self._margin_positions = {(self.as_common_symbol(r["pair"]), r["position_side"]): float(r["open_amount"]) for r in res["data"]["positions"]}
            self.logger.info(f"Updated margin position cache (REST) -- {self._margin_positions}")
            return res["data"]

    async def fetch_account_balance(self):
        res = await self._request("GET", path="/v1/user/assets")
        if res.get("success") != 1:
            self.logger.warning(f"APIError: {self.get_error_cause(res)}")
        return res["data"]

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

    async def on_message(self, msg: Dict):
        if "_data_type" not in msg:
            return
        dtype = msg.get("_data_type")

        if dtype in ("Order", "Trade") and "streaming" in msg.get("misc") and msg.get("venue") == "bitbank":
            self._last_stream_msg_timestamp = datetime.datetime.now()
        elif dtype == "PositionUpdate" and msg.get("venue") == "bitbank":
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
                await self.strategy.on_message(oinfo.to_dict(to_string_timestamp=False))  # invoke strategy
                asyncio.create_task(self.ticker_plant.info(json.dumps(oinfo.to_dict())))  # store in TP
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
                await self.strategy.on_message(trade.to_dict(to_string_timestamp=False))  # invoke strategy
                asyncio.create_task(self.ticker_plant.info(json.dumps(trade.to_dict())))  # store in TP
                self.logger.info(f"Found new transaction: \n{trade.to_dict()}")
                self.logger.info(f"Current position = {self.positions}")

        if dtype == "PositionUpdate":
            self._margin_positions[(msg["sym"], msg["position_side"])] = msg["open_amount"]
            self.logger.info(f'Updated margin position cache (Streaming) -- {msg}')

        # pop older cache
        t_theta = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=14)
        if len(self._order_cache) > 10000:
            self._order_cache = {k: o for k, o in self._order_cache.items() if t_theta < o.timestamp}
        if len(self._transaction_cache) > 10000:
            self._transaction_cache = {k: t for k, t in self._transaction_cache.items() if t_theta < t.timestamp}

    async def fetch_ticker(self, return_raw_response=True):
        async with aiohttp.ClientSession() as session:
            async with session.get("https://public.bitbank.cc/tickers") as resp:
                resp.raise_for_status()
                if return_raw_response:
                    return await resp.json()
                else:
                    res = await resp.json()
                    df = (
                        pd.DataFrame(res["data"])
                        .assign(
                            timestamp=lambda x: pd.to_datetime(x.timestamp*1e6),
                            pair=lambda x: x.pair.str.replace("_", "").str.upper(),
                        )
                        .set_index(["pair", "timestamp"]).astype(float)
                    )
                    return df
