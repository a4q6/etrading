import asyncio
import websockets
import json
import numpy as np
import datetime
import pytz
from typing import Callable, Awaitable, Optional, List, Dict
from sortedcontainers import SortedDict
from uuid import uuid4
from copy import deepcopy
from pathlib import Path

from etr.core.async_logger import AsyncBufferedLogger
from etr.config import Config
from etr.core.datamodel import MarketBook, MarketTrade, Rate, VENUE
from etr.common.logger import LoggerFactory


class BitBankSocketClient:
    def __init__(
        self,
        ccy_pairs: List[str] = ["btc_jpy"],
        callbacks: List[Callable[[dict], Awaitable[None]]] = [],
        reconnect_attempts: Optional[int] = None,  # no limit
    ):
        self.ws_url = "wss://stream.bitbank.cc/socket.io/?EIO=4&transport=websocket"
        self.callbacks = callbacks

        # logger
        log_file = Path(Config.LOG_DIR).joinpath("main.log").as_posix()
        tp_file = Path(Config.TP_DIR)
        self.ticker_plant: Dict[str, AsyncBufferedLogger] = {
            ccy_pair: AsyncBufferedLogger(logger_name=f"TP-{self.__class__.__name__}-{ccy_pair}", log_dir=tp_file.as_posix())
            for ccy_pair in ccy_pairs
        }
        self.logger = LoggerFactory().get_logger(logger_name="main", log_file=log_file)
        
        # status flags
        self._ws = None
        self._connected = False
        self._running = True
        self.reconnect_attempts = reconnect_attempts

        # channels
        self.channels = []
        self.market_book: Dict[str, MarketBook] = {}
        self.rate: Dict[str, Rate] = {}
        self.diff_message_buffer: Dict[str, SortedDict] = {}
        self.last_emit_market_book = {}
        for ccy_pair in ccy_pairs:
            self.channels.append(f"transactions_{ccy_pair}")
            self.channels.append(f"depth_whole_{ccy_pair}")
            self.channels.append(f"depth_diff_{ccy_pair}")
            self.channels.append(f"circuit_break_info_{ccy_pair}")
            self.market_book[ccy_pair] = MarketBook(sym=ccy_pair.replace("_", "").upper(), venue=VENUE.BITBANK, misc=["null", 0])
            self.rate[ccy_pair] = Rate(sym=ccy_pair.replace("_", "").upper(), venue=VENUE.BITBANK)
            self.diff_message_buffer[ccy_pair] = SortedDict()
            self.last_emit_market_book[ccy_pair] = datetime.datetime(2000, 1, 1, tzinfo=pytz.timezone("UTC"))

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
                sleep_sec = 10 * np.log(self.attempts)
                self.logger.info(f"Wait {round(sleep_sec, 2)} seconds to reconnect...")
                await asyncio.sleep(sleep_sec)

    async def _connect(self):
        async with websockets.connect(self.ws_url) as websocket:
            self._connected = True
            self._ws = websocket
            self.logger.info(f"Start subscribing: {self.ws_url}")
            try:
                while self._connected and self._running:
                    raw_msg = await websocket.recv()
                    if raw_msg.startswith("0"):
                        self.logger.info(f"Handshake: {raw_msg}")
                        await websocket.send("40")
                    elif raw_msg == "2":
                        # ping
                        self.logger.info("Response to ping(2) -> pong(3)")
                        await websocket.send("3")  # send pong
                        continue
                    elif raw_msg.startswith("40"):
                        self.logger.info(f"Connection established: {raw_msg}")
                        self.attempts = 0  # reset retry counts
                        for channel in self.channels:
                            subscribe_msg = f'42["join-room","{channel}"]'
                            await self._ws.send(subscribe_msg)
                            self.logger.info(f"Send request for '{channel}'")
                    elif raw_msg.startswith("42"):
                        await self._process_message(raw_msg)
                    else:
                        self.logger.info(f"Uncategorized message : {raw_msg}")

            except websockets.exceptions.ConnectionClosedOK:
                self.logger.info("Websoket closed OK")
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

    async def _process_message(self, raw_msg: str):
        
        # extract message body
        body = None
        try:
            payload = json.loads(raw_msg[2:])
            if isinstance(payload, list) and len(payload) == 2 and payload[0] == "message":
                body = payload[1]
        except json.JSONDecodeError:
            self.logger.warning(f"Failed to parse: {raw_msg}")
            return  # parse error

        # invalid message
        if "room_name" not in body.keys():
            return 

        # folk by message type
        ccypair = "_".join(body["room_name"].split("_")[-2:])
        if body["room_name"].startswith("transaction"):
            transactions = body["message"]["data"]["transactions"]
            for trs in transactions:
                # {"transaction_id":1201050197,"side":"buy","price":"12113485","amount":"0.0020","executed_at":1744941514688}
                data = MarketTrade(
                    timestamp=datetime.datetime.now(datetime.timezone.utc),
                    market_created_timestamp=datetime.datetime.fromtimestamp(trs["executed_at"] / 1e3, tz=pytz.timezone("Asia/Tokyo")),
                    sym=ccypair.replace("_", "").upper(),
                    venue=VENUE.BITBANK,
                    side=+1 if trs["side"] == 'buy' else -1,
                    price=float(trs["price"]),
                    amount=float(trs["amount"]),
                    trade_id=str(trs["transaction_id"]),
                )
                if self.callbacks: asyncio.create_task(asyncio.gather(*[callback(data) for callback in self.callbacks]))  # send(wo-awaiting)
                asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(data.to_dict()))) # store

        elif body["room_name"].startswith("depth_whole"):
            # {"asks":[["12106401","0.0100"],["12106419","0.0079"],["12106420","0.0030"],["12107601","0.0030"],["12108072","0.5000"],["12108691","0.0400"],["12110427","0.0045"],["12110429","0.0015"],["12110695","0.0011"],["12110810","0.0200"],["12111105","1.2277"],["12111318","0.0100"],["12112245","0.0063"],["12114038","0.0233"],["12114878","0.0052"],["12115295","0.0233"],["12116248","0.0005"],["12116562","0.0100"],["12116616","0.0227"],["12117584","0.5000"],["12117832","0.0222"],["12118593","0.0001"],["12119214","0.0400"],["12119730","0.0400"],["12120000","0.0010"],["12121901","0.0100"],["12123163","0.0001"],["12123778","0.0073"],["12123779","0.0400"],["12125405","0.0088"],["12133236","0.0022"],["12137286","0.8302"],["12139647","0.4002"],["12140000","0.0071"],["12141836","0.0001"],["12142554","1.0600"],["12149899","0.0050"],["12150000","0.1624"],["12150046","0.0121"],["12150070","0.0200"],["12150214","0.0003"],["12150422","0.0003"],["12160000","0.0005"],["12160001","0.0061"],["12167290","0.0013"],["12167605","0.0013"],["12172652","0.0003"],["12174899","0.0132"],["12181534","0.0005"],["12185740","0.0001"],["12189991","0.1040"],["12190000","0.0490"],["12199899","0.0050"],["12199988","0.0100"],["12199999","0.8897"],["12200000","2.7456"],["12201750","0.1234"],["12203000","0.0100"],["12209500","0.0001"],["12209689","0.0002"],["12210000","0.0001"],["12213322","0.0004"],["12214368","0.0005"],["12215058","0.0001"],["12215755","0.0005"],["12217613","0.0001"],["12219485","0.0010"],["12220000","0.5938"],["12222200","0.0003"],["12222800","0.0001"],["12224899","0.0100"],["12225000","1.7803"],["12229413","0.0247"],["12230000","0.0104"],["12235759","0.0001"],["12237128","0.0001"],["12238071","1.8000"],["12239056","0.0002"],["12240000","0.0505"],["12240001","0.0002"],["12246022","0.0013"],["12246342","0.0013"],["12247000","0.0001"],["12249110","0.0001"],["12249899","0.0100"],["12250000","0.5134"],["12250100","0.6000"],["12253521","0.0100"],["12254098","0.0005"],["12255000","0.0030"],["12255874","0.0001"],["12260000","0.0087"],["12260001","0.0988"],["12260334","0.0001"],["12266551","0.0001"],["12270000","0.0518"],["12277860","0.0128"],["12280000","0.3957"],["12288662","0.0814"],["12290603","0.0001"],["12290892","0.0016"],["12293492","0.0004"],["12295036","0.0005"],["12299899","0.0100"],["12300000","1.0670"],["12300004","0.0509"],["12304355","0.0001"],["12305600","0.0025"],["12309980","0.0100"],["12310000","0.0010"],["12312500","0.3638"],["12315500","0.0002"],["12319124","0.0001"],["12320000","0.0487"],["12321971","0.5551"],["12326987","0.0005"],["12330000","0.0146"],["12330637","0.0007"],["12331320","0.1000"],["12333000","0.2412"],["12333335","0.0001"],["12334315","0.0008"],["12340000","0.0090"],["12345678","0.0385"],["12347000","0.0001"],["12349899","0.0100"],["12350000","0.5672"],["12360000","0.0059"],["12363489","0.0002"],["12367925","0.0005"],["12370000","0.0070"],["12374000","0.0030"],["12379262","0.0013"],["12379587","0.0013"],["12380000","0.0975"],["12383210","0.0017"],["12386000","0.0001"],["12386002","0.0089"],["12388306","0.0075"],["12388888","0.0100"],["12390000","0.0020"],["12395000","0.0024"],["12399800","0.0001"],["12399899","0.0100"],["12399999","0.0145"],["12400000","1.1756"],["12400020","0.0200"],["12406200","0.0256"],["12407654","0.0003"],["12410000","0.0322"],["12419954","0.0027"],["12436480","0.0092"],["12440000","0.0010"],["12440001","0.0554"],["12447000","0.0002"],["12448102","0.4151"],["12449899","0.0100"],["12450000","0.2126"],["12454162","0.0001"],["12456789","0.0803"],["12474250","0.0100"],["12475503","0.0075"],["12480000","0.0025"],["12490000","0.1161"],["12491038","0.0004"],["12492404","0.0067"],["12493888","0.0004"],["12497000","0.0121"],["12498124","0.0004"],["12499000","0.0006"],["12499899","0.0100"],["12499999","0.0524"],["12500000","2.9533"],["12500500","0.0202"],["12501915","0.0003"],["12502000","0.0100"],["12503890","0.0049"],["12504908","0.0010"],["12510000","0.0029"],["12520000","0.0033"],["12525001","0.0548"],["12536083","0.0004"],["12541000","0.0003"],["12548000","0.2393"],["12548710","0.0005"],["12549899","0.0100"],["12550000","0.0712"],["12550147","0.0031"],["12554000","0.0403"],["12554764","0.0010"],["12555555","0.0082"],["12555690","0.0002"],["12560000","0.0008"],["12562658","0.0100"],["12565377","0.1000"],["12570001","0.0006"],["12580000","0.0238"],["12584688","0.0100"],["12588000","0.1908"],["12591380","0.0003"]],"bids":[["12106400","1.7047"],["12105848","0.4250"],["12105814","0.0580"],["12105806","0.0228"],["12105731","0.0320"],["12105138","0.0125"],["12105137","0.1500"],["12105028","0.0282"],["12104817","0.0667"],["12103271","0.0011"],["12101947","0.0040"],["12101926","0.0030"],["12101645","0.0091"],["12100598","0.0014"],["12100249","0.0030"],["12100000","0.0006"],["12099000","0.0010"],["12098986","0.0200"],["12096962","0.0045"],["12096960","0.0100"],["12096290","0.1600"],["12096074","0.0200"],["12096064","0.0052"],["12095989","0.0400"],["12095738","0.0001"],["12095000","0.0416"],["12094566","0.0200"],["12093147","0.0400"],["12092562","0.0496"],["12091858","0.0001"],["12090687","0.0100"],["12090243","0.0001"],["12090000","0.0646"],["12089570","0.0100"],["12089083","0.0088"],["12088973","0.0100"],["12088888","0.0008"],["12088047","0.0400"],["12086882","0.0400"],["12085015","0.0001"],["12083984","0.0001"],["12082223","0.0005"],["12080000","0.0206"],["12077592","0.0004"],["12076000","0.0002"],["12074016","0.5984"],["12074011","0.8318"],["12072421","0.4075"],["12071235","0.0073"],["12071234","0.0200"],["12068335","0.0002"],["12066999","0.0016"],["12064610","0.0001"],["12062596","0.0005"],["12061526","0.0001"],["12061032","0.2000"],["12060834","0.1700"],["12060000","0.0204"],["12057906","0.0001"],["12050892","0.0008"],["12050722","0.0002"],["12050435","0.0015"],["12050120","0.0015"],["12050000","0.0030"],["12046888","0.0200"],["12044503","0.0003"],["12043779","0.0113"],["12040000","0.0002"],["12037287","0.0002"],["12020000","0.0102"],["12019249","0.0001"],["12012139","0.2000"],["12011234","0.0001"],["12011233","0.0311"],["12005555","0.0001"],["12005500","0.0001"],["12003001","0.0001"],["12001020","0.0017"],["12000009","0.0034"],["12000001","0.0866"],["12000000","0.3749"],["11999999","0.1001"],["11998990","0.0018"],["11992174","0.0002"],["11991856","0.0010"],["11991199","0.0001"],["11990000","0.4628"],["11986590","0.0002"],["11984737","0.0032"],["11980674","0.0001"],["11980000","0.0138"],["11977777","0.3058"],["11974508","0.0005"],["11971757","0.0015"],["11971442","0.0015"],["11971234","0.0074"],["11970550","0.0002"],["11960000","0.0150"],["11959000","0.0010"],["11953838","0.0002"],["11952268","0.0001"],["11950088","0.0150"],["11950000","0.7088"],["11948996","0.0030"],["11947253","0.0256"],["11940000","0.0661"],["11938888","0.0100"],["11935370","0.0001"],["11934916","0.0005"],["11933524","0.0002"],["11931373","0.0001"],["11930009","0.0500"],["11930000","0.1020"],["11929980","0.0020"],["11926888","0.0100"],["11925250","0.0001"],["11923000","0.0010"],["11920000","0.0251"],["11918152","0.0008"],["11917895","0.4897"],["11917688","0.6204"],["11916795","0.6584"],["11916060","0.4455"],["11915959","0.0850"],["11915187","0.3226"],["11914893","0.0033"],["11914458","0.7238"],["11913978","0.3025"],["11912323","0.3521"],["11911980","0.0013"],["11911036","0.0100"],["11910000","0.0797"],["11904941","0.0004"],["11903771","0.0003"],["11902166","0.0001"],["11902001","0.1010"],["11901234","0.0072"],["11900001","0.0149"],["11900000","1.1402"],["11899999","0.0085"],["11897000","0.0100"],["11896763","0.0009"],["11896719","0.0001"],["11895000","0.0835"],["11894429","0.0008"],["11890510","0.0016"],["11890421","0.0001"],["11890000","0.0077"],["11888880","0.0005"],["11888000","0.1100"],["11886494","0.0008"],["11886038","0.0050"],["11885120","0.0500"],["11883383","0.0002"],["11883039","0.0001"],["11883000","0.0010"],["11881088","0.0001"],["11880088","0.0150"],["11880000","0.5490"],["11879970","0.0030"],["11876036","0.0100"],["11875500","0.0060"],["11872233","0.0100"],["11870420","0.0002"],["11870000","0.0010"],["11869850","0.0001"],["11869569","0.0042"],["11867000","0.0001"],["11865522","0.0100"],["11863783","0.0001"],["11863700","0.4930"],["11860000","0.0358"],["11858007","0.2500"],["11857000","0.0001"],["11856550","0.0500"],["11855426","0.0050"],["11855425","0.0030"],["11855229","0.0016"],["11855000","0.0002"],["11853366","0.0100"],["11852765","0.0002"],["11851698","0.0049"],["11850001","0.0001"],["11850000","0.4095"],["11846655","0.0100"],["11846092","0.0001"],["11842290","0.0010"],["11841036","0.0100"],["11840000","0.0316"],["11839800","0.0017"],["11838602","0.0015"],["11838292","0.0015"],["11837722","0.0100"],["11837218","0.1800"],["11836565","0.0176"],["11836188","0.0001"],["11831234","0.0046"],["11830000","0.0004"],["11827247","0.0100"],["11826000","0.0069"]],"asks_over":"218.1821","bids_under":"20826.1782","asks_under":"0.0000","bids_over":"0.0000","ask_market":"0.0000","bid_market":"0.0000","timestamp":1744948051363,"sequenceId":"24791326728"}}}
            book_msg = transactions = body["message"]["data"]
            sequence_id = int(book_msg["sequenceId"])
            # refresh market book
            cur_book = self.market_book[ccypair]
            cur_book.timestamp = datetime.datetime.now(datetime.timezone.utc)
            cur_book.market_created_timestamp = datetime.datetime.fromtimestamp(book_msg["timestamp"] / 1e3, tz=pytz.timezone("Asia/Tokyo"))
            cur_book.bids = SortedDict({float(price): float(amt) for price, amt in book_msg["bids"]})
            cur_book.asks = SortedDict({float(price): float(amt) for price, amt in book_msg["asks"]})
            cur_book.universal_id = uuid4().hex
            cur_book.misc = ["whole", sequence_id]
            # discard older diff msg
            self.diff_message_buffer[ccypair] = SortedDict({s_id: msg for s_id, msg in self.diff_message_buffer[ccypair].items() if sequence_id < s_id})
            # reflect diff msg
            for s_id, msg in self.diff_message_buffer[ccypair].items():
                cur_book.timestamp = datetime.datetime.fromtimestamp(msg["t"] / 1e3)
                for diff in msg["a"]:
                    price, amt = float(diff[0]), float(diff[1])
                    if amt == 0 and price in cur_book.asks.keys():
                        cur_book.asks.pop(price)
                    else:
                        cur_book.asks[price] = amt
                for diff in msg["b"]:
                    price, amt = float(diff[0]), float(diff[1])
                    if amt == 0 and price in cur_book.bids.keys():
                        cur_book.bids.pop(price)
                    else:
                        cur_book.bids[price] = amt
                cur_book.misc = ["whole", s_id]
            self.market_book[ccypair] = deepcopy(cur_book)
            
            # distribute
            if self.callbacks: asyncio.create_task(asyncio.gather(*[callback(deepcopy(cur_book)) for callback in self.callbacks]))  # send(wo-awaiting)
            asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(cur_book.to_dict())))  # store

        elif body["room_name"].startswith("depth_diff"):
            # {'a': [['12107601', '0.006']], 'b': [['12105138', '0.025'], ['12105137', '0']], 't': 1744948051858, 's': '24791326838', 'ao': '218.1821', 'bu': '20826.1782'}}
            msg = body["message"]["data"]
            sequence_id = int(msg["s"])
            self.diff_message_buffer[ccypair][sequence_id] = msg  # save in buffer
            cur_book = self.market_book[ccypair]
            if cur_book.misc[0] != "null":  # make sure market book is initialized with "depth_whole" message
                last_update = cur_book.timestamp
                cur_book.universal_id = uuid4().hex
                if cur_book.misc[-1] < sequence_id:
                    cur_book.timestamp = datetime.datetime.now(datetime.timezone.utc)
                    cur_book.market_created_timestamp = datetime.datetime.fromtimestamp(msg["t"] / 1e3, tz=pytz.timezone("Asia/Tokyo"))
                    for diff in msg["a"]:
                        price, amt = float(diff[0]), float(diff[1])
                        if amt == 0 and price in cur_book.asks.keys():
                            cur_book.asks.pop(price)
                        elif amt != 0:
                            cur_book.asks[price] = amt
                    for diff in msg["b"]:
                        price, amt = float(diff[0]), float(diff[1])
                        if amt == 0 and price in cur_book.bids.keys():
                            cur_book.bids.pop(price)
                        elif amt != 0:
                            cur_book.bids[price] = amt
                    cur_book.misc = ["diff", sequence_id]
                self.market_book[ccypair] = deepcopy(cur_book)

                # distribute
                if self.callbacks: asyncio.create_task(asyncio.gather(*[callback(deepcopy(cur_book)) for callback in self.callbacks]))  # send(wo-awaiting)
                if self.last_emit_market_book[ccypair] + datetime.timedelta(milliseconds=250) < cur_book.timestamp:
                    asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(cur_book.to_dict())))  # store
                    self.last_emit_market_book[ccypair] = cur_book.timestamp

        elif body["room_name"].startswith("circuit_break_info"):
            msg = body["message"]["data"]
            msg["_data_type"] = "CircuitBreaker"
            msg["received_timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            msg["timestamp"] = datetime.datetime.fromtimestamp(msg["timestamp"] / 1e3, tz=pytz.timezone("Asia/Tokyo")).isoformat()
            msg["sym"] = ccypair.replace("_", "").upper()
            msg["venue"] = VENUE.BITBANK
            asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(msg)))  # store

        if body["room_name"].startswith("depth"):
            # Create Rate data from local market book
            cur_book = self.market_book[ccypair]
            if cur_book.misc[0] != "null":
                new_rate = cur_book.to_rate()
                if self.rate[ccypair].mid_price != new_rate.mid_price:
                    self.rate[ccypair] = new_rate
                    if self.callbacks: asyncio.create_task(asyncio.gather(*[callback(deepcopy(new_rate)) for callback in self.callbacks]))
                    asyncio.create_task(self.ticker_plant[ccypair].info(json.dumps(new_rate.to_dict())))  # store

if __name__ == '__main__':
    client = BitBankSocketClient()
    try:
        asyncio.run(client.start())
    except KeyboardInterrupt:
        print("Disconnected")
        asyncio.run(client.close())
