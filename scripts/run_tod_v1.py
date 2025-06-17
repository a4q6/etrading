import asyncio
import datetime
import pandas as pd
from etr.core.ws import LocalWsClient
from etr.strategy.tod.tod_v1 import TOD_v1
from etr.core.api_client import CoincheckRestClient


if __name__ == "__main__":

    # global COUNTER
    # COUNTER = 0
    # async def my_print(msg):
    #     print(msg)
    #     global COUNTER
    #     COUNTER += 1
    #     if COUNTER > 10:
    #         raise RuntimeError()

    # initialize
    client = CoincheckRestClient(log_file="tod_v1.log")
    strategy = TOD_v1(
        venue="coincheck",
        entry_config=[
            {"sym": "XRPJPY", "start": datetime.time(1, 29), "holding_minutes": 60 * 4, "sl_level": 300, "side": 1, "amount": 10, "spread_filter": 50},
            {"sym": "XRPJPY", "start": datetime.time(6, 59), "holding_minutes": 60 * 5 + 30, "sl_level": 300, "side": 1, "amount": 10, "spread_filter": 50},
            {"sym": "XRPJPY", "start": datetime.time(16, 29), "holding_minutes": 60 * 4, "sl_level": 300, "side": 1, "amount": 5, "spread_filter": 50},
        ],
        client=client, 
        log_file="tod_v1.log")
    strategy.warmup()
    client.register_strategy(strategy)
    subscriber = LocalWsClient(callbacks=[strategy.on_message], log_file="tod_v1.log")
    # subscriber = LocalWsClient(callbacks=[my_print], log_file="tod_v1.log")

    # ws subsciber loop
    async def start_subscribe():
        await subscriber.connect()
        await asyncio.sleep(1)
        await subscriber.subscribe([
            {"_data_type": "Rate", "venue": "coincheck", "sym": "XRPJPY"},
            {"_data_type": "MarketTrade", "venue": "coincheck", "sym": "XRPJPY"},
        ])
        asyncio.create_task(client.loop_fetch_transactions())

        now = pd.Timestamp.today()
        # stop = (now + pd.Timedelta("1d")).ceil("1d")
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
