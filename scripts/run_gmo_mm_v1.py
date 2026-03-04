import asyncio
import pandas as pd
from etr.core.ws import LocalWsClient
from etr.core.api_client.gmo import GmoRestClient
from etr.strategy.gmo_mm.gmo_mm_v1 import GmoMMv1

LOG_FILE = "gmo_mm_v1.log"
SYM = "BTCJPY"
VENUE = "gmo"

if __name__ == "__main__":

    # Initialize REST client
    client = GmoRestClient(log_file=LOG_FILE)
    # pending_limit_orders is used by the strategy to guard against duplicate orders.
    # GmoRestClient tracks orders via _order_cache; expose an empty dict as a shim.
    client.pending_limit_orders = {}

    # Initialize strategy
    strategy = GmoMMv1(
        sym=SYM,
        venue=VENUE,
        amount=0.001,           # order size (BTC)
        client=client,
        # Spread parameters
        base_offset=2.0,
        vol_coef=0.5,
        adverse_coef=1.0,
        min_spread=1.0,
        max_spread=10.0,
        # Inventory
        skew_coef=2.0,
        max_position=0.005,
        # Time management
        position_horizon=120,
        # Adverse selection
        adverse_threshold=3.0,
        # Reference markets (default: bitflyer FXBTCJPY, binance BTCUSDT, bitmex XBTUSD)
        reference=[
            ("bitflyer", "FXBTCJPY"),
            ("binance",  "BTCUSDT"),
            ("bitmex",   "XBTUSD"),
        ],
        # Vol filter
        vol_threshold=100,
        log_file=LOG_FILE,
    )

    strategy.warmup()
    client.register_strategy(strategy)

    # LocalWsClient receives messages from LocalWsPublisher (port 8765)
    subscriber = LocalWsClient(callbacks=[strategy.on_message], log_file=LOG_FILE)

    async def start_subscribe():
        await subscriber.connect()
        await asyncio.sleep(1)
        await subscriber.subscribe([
            # Target market
            {"_data_type": "Rate",        "venue": VENUE,       "sym": SYM},
            {"_data_type": "MarketBook",  "venue": VENUE,       "sym": SYM},
            {"_data_type": "MarketTrade", "venue": VENUE,       "sym": SYM},
            # FX rate for USD→JPY conversion
            {"_data_type": "Rate",        "venue": VENUE,       "sym": "USDJPY"},
            # Cross-venue reference markets
            {"_data_type": "Rate",        "venue": "bitflyer",  "sym": "FXBTCJPY"},
            {"_data_type": "Rate",        "venue": "binance",   "sym": "BTCUSDT"},
            {"_data_type": "Rate",        "venue": "bitmex",    "sym": "XBTUSD"},
            # Private stream: order / execution events
            {"_data_type": "Order"},
            {"_data_type": "Trade"},
        ])

        # Keep alive for ~10 years
        await asyncio.sleep(60 * 60 * 24 * 365 * 10)

    async def main():
        try:
            await start_subscribe()
        except KeyboardInterrupt:
            print("Interrupted by user")
        finally:
            print("Closing connections...")
            await subscriber.close()

    asyncio.run(main())
