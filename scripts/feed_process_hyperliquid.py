import asyncio
from etr.core.ws import LocalWsPublisher
from etr.core.feed_handler.hyperliquid import HyperliquidSocketClient


if __name__ == '__main__':

    async def main():
        # initialize clients
        # publisher = LocalWsPublisher(port=8765)
        hyperliquid = HyperliquidSocketClient(ccy_pairs=["HYPE", "ETH", "BTC", "HYPEUSDC", "ETHUSDC", "SOLUSDC"], limit_candle_symbols=False)

        try:
            await asyncio.gather(
                hyperliquid.start(),
            )

        except KeyboardInterrupt:
            print("Interrupted by user")

        finally:
            print("Closing connections...")
            await asyncio.gather(
                hyperliquid.close(),
            )

    asyncio.run(main())
