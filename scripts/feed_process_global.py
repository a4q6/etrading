import asyncio
from etr.core.ws import LocalWsPublisher
from etr.core.feed_handler.coinbase import CoinbaseSocketClient
from etr.core.feed_handler.bybit import BybitLinearSocketClient
from etr.core.feed_handler.okx import OkxSwapSocketClient


if __name__ == '__main__':

    async def main():
        # publisher = LocalWsPublisher(port=8765)
        coinbase = CoinbaseSocketClient(
            ccy_pairs=["BTC-USD", "ETH-USD", "XRP-USD", "SOL-USD", "DOGE-USD", "LTC-USD"],
            # publisher=publisher,
        )
        bybit_linear = BybitLinearSocketClient(
            ccy_pairs=["BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "DOGEUSDT", "LTCUSDT"],
            # publisher=publisher,
        )
        okx = OkxSwapSocketClient(
            ccy_pairs=["BTC-USDT-SWAP", "ETH-USDT-SWAP", "XRP-USDT-SWAP", "SOL-USDT-SWAP", "DOGE-USDT-SWAP", "LTC-USDT-SWAP"],
            # publisher=publisher,
        )

        try:
            await asyncio.gather(
                # publisher.start(),
                coinbase.start(),
                bybit_linear.start(),
                okx.start(),
            )

        except KeyboardInterrupt:
            print("Interrupted by user")

        finally:
            print("Closing connections...")
            await asyncio.gather(
                coinbase.close(),
                bybit_linear.close(),
                okx.close(),
            )

    asyncio.run(main())
