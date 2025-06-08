import asyncio
from etr.core.ws import LocalWsPublisher
from etr.core.feed_handler.bitbank import BitBankSocketClient
from etr.core.feed_handler.bitmex import BitmexSocketClient
from etr.core.feed_handler.bitflyer import BitFlyerSocketClient, BitFlyerFundingRate
from etr.core.feed_handler.gmo_forex import GmoForexSocketClient
from etr.core.feed_handler.gmo_crypt import GmoCryptSocketClient
from etr.core.feed_handler.coincheck import CoincheckSocketClient
from etr.core.feed_handler.binance import BinanceSocketClient
from etr.core.feed_handler.binance_eoption import BinanceRestEoption


if __name__ == '__main__':
    publisher = LocalWsPublisher(port=8765)
    bitbank = BitBankSocketClient(ccy_pairs=["btc_jpy", "xrp_jpy", "eth_jpy", "doge_jpy", "ltc_jpy", "sol_jpy"], publisher=publisher)
    bitmex = BitmexSocketClient(ccy_pairs=["ETHUSD", "XBTUSD", "XRPUSD", "LTCUSD", "DOGEUSD", "SOLUSD", "BTCUSD"], publisher=publisher)
    bitflyer = BitFlyerSocketClient(ccy_pairs=["FX_BTC_JPY", "BTC_JPY", "ETH_JPY", "XRP_JPY", "ETH_BTC"], publisher=publisher)
    bitflyer_fr = BitFlyerFundingRate(ccy_pairs=["FX_BTC_JPY"])
    gmo_fx = GmoForexSocketClient(ccy_pairs=["USD_JPY", "EUR_USD", "GBP_USD", "AUD_USD", "EUR_JPY", "GBP_JPY", "CHF_JPY", "CAD_JPY", "AUD_JPY"], publisher=publisher)
    gmo_cr = GmoCryptSocketClient(ccy_pairs=["BTC", "ETH", "XRP", "LTC", "DOGE", "SOL", "BTC_JPY", "ETH_JPY", "XRP_JPY", "LTC_JPY", "DOGE_JPY", "SOL_JPY"], publisher=publisher)
    coincheck = CoincheckSocketClient(ccy_pairs=["btc_jpy", "eth_jpy", "xrp_jpy", "doge_jpy"], publisher=publisher)
    binance = BinanceSocketClient(ccy_pairs=["BTCUSDT", "ETHUSDT", "XRPUSDT", "LTCUSDT", "SOLUSDT", "DOGEUSDT"], publisher=publisher)
    binance_opt = BinanceRestEoption(ccy_pairs=["BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "DOGEUSDT"], publisher=publisher)

    async def main():
        try:
            await asyncio.gather(
                publisher.start(),
                gmo_fx.start(),
                bitbank.start(),
                bitmex.start(),
                bitflyer.start(),
                gmo_cr.start(),
                coincheck.start(),
                bitflyer_fr.start(),
                binance.start(),
                binance_opt.start(),
            )
        except KeyboardInterrupt:
            print("Interrupted by user")
        finally:
            print("Closing connections...")
            await asyncio.gather(
                publisher.close(),
                bitbank.close(),
                bitmex.close(),
                bitflyer.close(),
                gmo_fx.close(),
                gmo_cr.close(),
                coincheck.close(),
                binance.close(),
                binance_opt.close()
            )

    asyncio.run(main())
