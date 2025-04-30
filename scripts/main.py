import asyncio
from etr.core.feed_handler.bitbank import BitBankSocketClient
from etr.core.feed_handler.bitmex import BitmexSocketClient
from etr.core.feed_handler.bitflyer import BitFlyerSocketClient


if __name__ == '__main__':
    bitbank = BitBankSocketClient(ccy_pairs=["btc_jpy", "xrp_jpy", "eth_jpy", "doge_jpy", "bcc_jpy", "ltc_jpy", "sol_jpy"])
    bitmex = BitmexSocketClient(ccy_pairs=["ETHUSD", "XBTUSD", "XRPUSD", "LTCUSD", "DOGEUSD", "BCHUSD", "SOLUSD"])
    bitflyer = BitFlyerSocketClient(ccy_pairs=["BTC_JPY", "ETH_JPY", "XRP_JPY"])

    async def main():
        try:
            await asyncio.gather(
                bitbank.start(),
                bitmex.start(),
                bitflyer.start(),
            )
        except KeyboardInterrupt:
            print("Interrupted by user")
        finally:
            print("Closing connections...")
            await asyncio.gather(
                bitbank.close(),
                bitmex.close(),
                bitflyer.close(),
            )

    asyncio.run(main())
