import asyncio
import datetime
import pandas as pd
import numpy as np
from etr.core.ws import LocalWsClient
from etr.strategy.imb_mm.imb_mm_bb import ImbMM_BB
from etr.core.api_client import BitbankRestClient
from etr.core.notification.discord import send_discord_webhook


if __name__ == "__main__":

    # initialize
    client = BitbankRestClient(log_file="imb_mm.log", tp_number=2)
    strategy = ImbMM_BB(
        sym="BTCJPY",
        venue="bitbank",
        amount=0.0001,
        decimal=0,
        spread_threshold=3.5,
        client=client,
        log_file="imb_mm.log"
    )
    client.register_strategy(strategy)
    subscriber = LocalWsClient(callbacks=[client.on_message, strategy.on_message], log_file="subscriber.log")

    # ws subsciber loop
    async def start_strategy():
        await strategy.warmup()
        await subscriber.connect()
        await asyncio.sleep(1)
        await subscriber.subscribe([
            # position data
            {"_data_type": "Order", "venue": "bitbank", "sym": "BTCJPY"},
            {"_data_type": "Trade", "venue": "bitbank", "sym": "BTCJPY"},
            {"_data_type": "PositionUpdate", "venue": "bitbank", "sym": "BTCJPY"},
            # market data
            {"_data_type": "Rate", "venue": "bitbank", "sym": "BTCJPY"},
            {"_data_type": "MarketBook", "venue": "bitbank", "sym": "BTCJPY"},
            {"_data_type": "Rate", "venue": "bitmex", "sym": "XBTUSD"},
            {"_data_type": "Rate", "venue": "binance", "sym": "BTCUSDT"},
        ])
        now = pd.Timestamp.today()
        stop = (now + pd.Timedelta("2H"))
        await asyncio.sleep((stop - now).total_seconds())
        # await asyncio.sleep(1 * 60 * 24 * 365 * 10)

    async def report(interval_min=60):
        while True:
            await asyncio.sleep(60 * interval_min)
            # Account Balance
            res = await client.fetch_account_balance()
            assets = pd.DataFrame(res["assets"])
            assets = assets.set_index("asset").iloc[:, :5].astype(float).query("onhand_amount > 0")
            send_discord_webhook("" + "\n" + str(assets.to_csv(sep="|")), username="BB-Account Balance")
            # Account
            res = await client.fetch_open_positions()
            pos = pd.DataFrame(res["positions"])
            pos = pos.set_index(["pair", "position_side"]).open_amount.unstack(level=-1).query("long != 0 or short !=0")
            send_discord_webhook("" + "\n" + str(pos.to_csv(sep="|")), username="BB-Open Positions")

    # start main logic
    async def main():
        try:
            await asyncio.gather(
                start_strategy(),
                report(),
            )
        except KeyboardInterrupt:
            print("Interrupted by user")
        except Exception as e:
            strategy.logger.error(f"{e}", exc_info=True)
        finally:
            print("Closing connections...")
            await asyncio.gather(
                subscriber.close(),
            )

    asyncio.run(main())
