import asyncio
from etr.core.feed_handler.bitbank import BitBankSocketClient
from etr.core.feed_handler.bitmex import BitmexSocketClient
from etr.core.feed_handler.bitflyer import BitFlyerSocketClient
from etr.core.feed_handler.gmo_forex import GmoForexSocketClient
from etr.core.feed_handler.gmo_crypt import GmoCryptSocketClient
from etr.core.feed_handler.coincheck import CoincheckSocketClient


if __name__ == '__main__':
    bitbank = BitBankSocketClient(ccy_pairs=["btc_jpy", "xrp_jpy", "eth_jpy", "doge_jpy", "ltc_jpy", "sol_jpy"])
    bitmex = BitmexSocketClient(ccy_pairs=["ETHUSD", "XBTUSD", "XRPUSD", "LTCUSD", "DOGEUSD", "SOLUSD", "BTCUSD"])
    bitflyer = BitFlyerSocketClient(ccy_pairs=["BTC_JPY", "ETH_JPY", "XRP_JPY"])
    gmo_fx = GmoForexSocketClient(ccy_pairs=["USD_JPY", "EUR_USD", "GBP_USD", "AUD_USD", "EUR_JPY", "GBP_JPY", "CHF_JPY", "CAD_JPY", "AUD_JPY"])
    gmo_cr = GmoCryptSocketClient(ccy_pairs=["BTC", "ETH", "XRP", "LTC", "DOGE", "SOL", "BTC_JPY", "ETH_JPY", "XRP_JPY", "LTC_JPY", "DOGE_JPY", "SOL_JPY"])
    coincheck = CoincheckSocketClient(ccy_pairs=["btc_jpy", "eth_jpy", "xrp_jpy", "doge_jpy"])

    async def main():
        try:
            await asyncio.gather(
                gmo_fx.start(),
                bitbank.start(),
                bitmex.start(),
                bitflyer.start(),
                gmo_cr.start(),
                coincheck.start(),
            )
        except KeyboardInterrupt:
            print("Interrupted by user")
        finally:
            print("Closing connections...")
            await asyncio.gather(
                bitbank.close(),
                bitmex.close(),
                bitflyer.close(),
                gmo_fx.close(),
                gmo_cr.close(),
                coincheck.close(),
            )

    asyncio.run(main())
