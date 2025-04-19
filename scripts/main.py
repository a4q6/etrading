import asyncio
from etr.core.feed_handler.bitbank import BitBankSocketClient
from etr.core.feed_handler.bitmex import BitmexSocketClient


if __name__ == '__main__':
    bitbank = BitBankSocketClient(ccy_pairs=["btc_jpy", "xrp_jpy", "eth_jpy", "doge_jpy", "bcc_jpy", "ltc_jpy"])
    bitmex = BitmexSocketClient(ccy_pairs=["ETHUSD", "XBTUSD", "XRPUSD", "LTCUSD", "DOGEUSD"])

    async def main():
        try:
            await asyncio.gather(
                bitbank.start(),
                bitmex.start()
            )
        except KeyboardInterrupt:
            print("Interrupted by user")
        finally:
            print("Closing connections...")
            await asyncio.gather(
                bitbank.close(),
                bitmex.close()
            )

    asyncio.run(main())
