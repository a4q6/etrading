import time, datetime
import numpy as np
import pandas as pd
import json
import itertools
from abc import abstractmethod, ABC
from typing import Dict, List, Tuple
from dataclasses import dataclass, asdict, field
from uuid import uuid4
from sortedcontainers import SortedDict


@dataclass
class VENUE:
    BITMEX: str = "bitmex"
    BITFLYER: str = "bitflyer"
    LIQU_id: str = "liquid"
    GAITAME: str = "gaitame"
    FTX: str = "ftx"
    BINANCE: str = "binance"
    COINCHECK: str = "coincheck"
    BITBANK: str = "bitbank"
    GMO: str = "gmo"


@dataclass
class MarketTrade:
    timestamp: datetime.datetime = pd.NaT
    market_created_timestamp: datetime.datetime = pd.NaT
    sym: str = None
    venue: str = None
    category: str = None
    side: int = None
    price: float = None
    amount: float = None
    trade_id: str = None
    order_ids: str = None
    misc: str = None
    universal_id: str = field(default_factory=lambda : uuid4().hex)

    def to_dict(self, to_string_timestamp=True):
        data = asdict(self)
        data["_data_type"] = self.__class__.__name__
        data["misc"] = str(data["misc"])
        if to_string_timestamp:
            data["timestamp"] = data["timestamp"].isoformat()
            data["market_created_timestamp"] = data["market_created_timestamp"].isoformat()
        return data
    
    @property
    def latency(self):
        return (self.timestamp - self.market_created_timestamp).total_seconds()

@dataclass
class Rate:
    timestamp: datetime.datetime = pd.NaT
    market_created_timestamp: datetime.datetime = pd.NaT
    sym: str = None
    venue: str = None
    category: str = None
    best_bid: float = np.nan
    best_ask: float = np.nan
    mid_price: float = np.nan
    misc: str = None
    universal_id: str = field(default_factory=lambda : uuid4().hex)

    def to_dict(self, to_string_timestamp=True):
        data = asdict(self)
        data["_data_type"] = self.__class__.__name__
        data["mid_price"] = (self.best_ask + self.best_bid) / 2
        data["misc"] = str(data["misc"])
        if to_string_timestamp:
            data["timestamp"] = data["timestamp"].isoformat()
            data["market_created_timestamp"] = data["market_created_timestamp"].isoformat()
        return data
    
    @property
    def latency(self):
        return (self.timestamp - self.market_created_timestamp).total_seconds()
        
    @property
    def spread(self):
        return self.best_ask - self.best_bid

@dataclass
class MarketBook:
    timestamp: datetime.datetime = pd.NaT
    market_created_timestamp: datetime.datetime = pd.NaT
    sym: str = None
    venue: str = None
    category: str = None
    bids: SortedDict = field(default_factory=SortedDict)
    asks: SortedDict = field(default_factory=SortedDict)
    universal_id: str = field(default_factory=lambda : uuid4().hex)
    misc: str = None

    def to_dict(self, level=20, to_string_timestamp=True):
        data = asdict(self)
        data["_data_type"] = self.__class__.__name__
        data["bids"] = list(data["bids"].items())[-level:][::-1]
        data["asks"] = list(data["asks"].items())[:level]
        data["misc"] = str(data["misc"])
        if to_string_timestamp:
            data["timestamp"] = data["timestamp"].isoformat()
            data["market_created_timestamp"] = data["market_created_timestamp"].isoformat()

        return data
    
    def to_rate(self) -> Rate:
        return Rate(
            timestamp=self.timestamp,
            market_created_timestamp=self.market_created_timestamp,
            best_bid=self.best_bid,
            best_ask=self.best_ask,
            mid_price=self.mid_price,
            sym=self.sym,
            venue=self.venue,
            category=self.category,
            misc=self.universal_id,
        )
    
    @property
    def best_bid(self):
        return self.bids.peekitem(-1)[0] if len(self.bids) > 0 else np.nan

    @property
    def best_ask(self):
        return self.asks.peekitem( 0)[0] if len(self.asks) > 0 else np.nan

    @property
    def mid_price(self):
        return (self.best_ask + self.best_bid) / 2

    @property
    def latency(self):
        return (self.timestamp - self.market_created_timestamp).total_seconds()

@dataclass
class Order:
    timestamp: datetime.datetime
    market_created_timestamp: datetime.datetime
    sym: str
    side: int
    price: float
    amount: float
    executed_amount: float
    order_type: str
    order_status: str
    venue: str
    order_id: str = None
    model_id: str = None
    process_id: str = None
    src_type: str = None
    src_id: str = None
    src_timestamp: datetime.datetime = None
    misc: str = None
    universal_id: str = field(default_factory=lambda : uuid4().hex)

    def to_dict(self, to_string_timestamp=True) -> Dict:
        data = asdict(self)
        data["_data_type"] = self.__class__.__name__
        if to_string_timestamp:
            data["timestamp"] = data["timestamp"].isoformat()
            data["market_created_timestamp"] = data["market_created_timestamp"].isoformat()
            data["src_timestamp"] = data["market_created_timestamp"].isoformat()
        return data

    @staticmethod
    def null_order() -> 'Order':
        return Order(
            timestamp=datetime.datetime(2000, 1, 1).astimezone(datetime.timezone.utc),
            market_created_timestamp=datetime.datetime(2000, 1, 1).astimezone(datetime.timezone.utc),
            sym="",
            side=0,
            price=np.nan,
            amount=np.nan,
            executed_amount=0,
            order_type="",
            order_status=OrderStatus.Canceled,
            venue="",
            universal_id="",
        )

    
@dataclass
class OrderType:
    Limit = "limit"
    Market = "market"
    @staticmethod
    def convert(s: str) -> str:
        if s in ["LIMIT"]:
            return OrderType.Limit
        elif s in ["MARKET"]:
            return OrderType.Market
        else:
            raise ValueError(f"Unexpected order type passed: {s}")
            
@dataclass
class OrderStatus:
    Null = ""
    New = "new"
    Updated = "updated"
    Partial = "partial"
    Canceled = "canceled"
    Filled = "filled"
    Pending = "pending"
    
    @staticmethod
    def convert(res: str = None, order = None) -> str:
        if order is None:
            order = Order.null_order()
        if res is None:
            return OrderStatus.New
        elif res in ("live", "ORDER", "EXECUTION") and not order.order_status in ("filled", "canceled"):
            return OrderStatus.Updated
        elif res in ("canceled", "cancelled", "CANCEL", "EXPIRE"):
            return OrderStatus.Canceled
        elif res in ("filled", ):
            return OrderStatus.Filled
        else:
            raise ValueError(f"`{res}` passed")

@dataclass
class Position:
    timestamp: datetime.datetime
    market_created_timestamp: datetime.datetime
    sym: str
    model_id: str
    process_id: str
    venue: str
    amount: float  # -inf ~ +inf
    cost: str
    pnl_closed: float = 0
    pnl_open: float = np.nan
    universal_id: str = None
    lastTrade_id: str = None
    @property
    def side(self):
        return np.sign(self.amount)
    @property
    def pnl_price(self):
        return self.pnl_open / abs(self.amount)


@dataclass
class Trade:
    timestamp: datetime.datetime
    market_created_timestamp: datetime.datetime
    sym: str
    venue: str
    side: int
    price: float
    amount: float  # > 0
    order_id: str
    order_type: str
    trade_id: str = None
    model_id: str = None
    process_id: str = None
    universal_id: str = None
    misc: str = None

    def to_dict(self, to_string_timestamp=True) -> Dict:
        data = asdict(self)
        data["_data_type"] = self.__class__.__name__
        if to_string_timestamp:
            data["timestamp"] = data["timestamp"].isoformat()
            data["market_created_timestamp"] = data["market_created_timestamp"].isoformat()
        return data

    @staticmethod
    def from_order(
        timestamp,
        market_created_timestamp,
        trade_id,
        price,
        exec_amount: float,
        order: Order,
        misc="",
    ) ->  'Trade':
        return Trade(
            timestamp, market_created_timestamp,
            sym=order.sym, venue=order.venue, side=order.side, price=price, amount=exec_amount,
            order_id=order.order_id, order_type=order.order_type, trade_id=trade_id, model_id=order.model_id, 
            process_id=order.process_id, universal_id=uuid4().hex, misc=misc,
        )
