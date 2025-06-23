import asyncio
import datetime
import pandas as pd
import numpy as np
from etr.core.ws import LocalWsClient
from etr.strategy.dev_mm.dev_mm_v1 import DevMMv1
from etr.core.api_client import BitbankRestClient

if __name__ == "__main__":

    # initialize
    client = BitbankRestClient(log_file="dev_mm.log")
    strategy = DevMMv1(
        sym="BTCJPY", venue="bitbank", amount=0.0001,
        position_horizon=60, 
        sl_level=100,
        tp_level=np.nan, 
        entry_offset=3.5,
        exit_offset=0, 
        dev_window=datetime.timedelta(minutes=10),
        reference=[('gmo', 'BTCJPY'), ('bitflyer', 'BTCJPY'), ('bitmex', 'XBTUSD'), ('binance', "BTCUSDT")],
        vol_threshold=50, decimal=0,
        client=client,
        log_file="dev_mm.log"
    )
    client.register_strategy(strategy)
    strategy.warmup()
    subscriber = LocalWsClient(callbacks=[strategy.on_message, client.on_message], log_file="dev_mm.log")

    # ws subsciber loop
    async def start_subscribe():
        await subscriber.connect()
        await asyncio.sleep(1)
        await subscriber.subscribe([
            {"_data_type": "Order", "venue": "bitbank", "sym": "BTCJPY"},
            {"_data_type": "Trade", "venue": "bitbank", "sym": "BTCJPY"},
            {"_data_type": "Rate", "venue": "bitbank", "sym": "BTCJPY"},
            {"_data_type": "Rate", "venue": "gmo", "sym": "USDJPY"},
            {"_data_type": "Rate", "venue": "gmo", "sym": "BTCJPY"},
            {"_data_type": "Rate", "venue": "bitflyer", "sym": "BTCJPY"},
            {"_data_type": "Rate", "venue": "bitmex", "sym": "XBTUSD"},
            {"_data_type": "Rate", "venue": "binance", "sym": "BTCUSDT"},
        ])
        # now = pd.Timestamp.today()
        # stop = (now + pd.Timedelta("1d")).ceil("1d")
        # await asyncio.sleep((stop - now).total_seconds())
        await asyncio.sleep(1 * 60 * 24 * 365 * 10)

    # start main logic
    async def main():
        try:
            await start_subscribe()
        except KeyboardInterrupt:
            print("Interrupted by user")
        finally:
            print("Closing connections...")
            await asyncio.gather(
                subscriber.close(),
            )

    asyncio.run(main())
