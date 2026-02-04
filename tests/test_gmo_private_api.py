"""
GMO Private REST/Streaming API Client Test Script

Test flow:
1. Receive Rate for GMO BTC_JPY via FeedHandler
2. Send limit order at 10% below current price
3. Cancel the order after 5+ seconds
4. Stop the process after another 3+ seconds
"""

import asyncio
import datetime
import json
from typing import Dict, Optional
from uuid import uuid4

from etr.strategy.base_strategy import StrategyBase
from etr.core.api_client.gmo import GmoRestClient
from etr.core.feed_handler.gmo_crypt import GmoCryptSocketClient
from etr.core.feed_handler.gmo_private_stream import GmoPrivateStreamClient
from etr.core.ws import LocalWsPublisher, LocalWsClient
from etr.core.datamodel import OrderType, OrderStatus, VENUE


class GmoTestStrategy(StrategyBase):
    """
    GMO API test strategy.

    Flow:
    1. Wait for Rate message
    2. Send limit order at 10% below mid price
    3. Cancel the order after 5 seconds
    4. Stop the process after 3 more seconds
    """

    def __init__(self, client: GmoRestClient, model_id: str = "gmo-test"):
        super().__init__(model_id=model_id, log_file="gmo_test.log")
        self.client = client
        self.client.register_strategy(self)

        # State management
        self._rate_received = False
        self._order_sent = False
        self._order_canceled = False
        self._pending_order: Optional[Dict] = None
        self._shutdown_requested = False

    async def on_message(self, msg: Dict):
        """
        Handle incoming messages.

        Supported message types:
        - Rate: Market price update
        - Order: Order status update
        - Trade: Execution notification
        """
        dtype = msg.get("_data_type")
        venue = msg.get("venue")

        # Only process GMO messages
        if venue != VENUE.GMO:
            return

        self.logger.debug(f"Received message: {dtype} - {msg}")

        if dtype == "Rate":
            await self._handle_rate(msg)
        elif dtype == "Order":
            await self._handle_order(msg)
        elif dtype == "Trade":
            await self._handle_trade(msg)

    async def _handle_rate(self, msg: Dict):
        """
        Handle Rate message.

        On first Rate received, send a limit order at 10% below current price.
        """
        if self._order_sent:
            return

        sym = msg.get("sym")
        if sym != "BTCJPY":
            return

        mid_price = msg.get("mid_price")
        if mid_price is None or mid_price <= 0:
            self.logger.warning(f"Invalid mid_price: {mid_price}")
            return

        self._rate_received = True
        self.logger.info(f"Received Rate: sym={sym}, mid_price={mid_price}")

        # Calculate order price (10% below current price)
        order_price = int(mid_price * 0.9)
        order_size = 0.001  # Minimum order size

        self.logger.info(f"Sending limit order: price={order_price}, size={order_size}")

        try:
            order = await self.client.send_order(
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                sym="BTCJPY",
                side=1,  # BUY
                price=order_price,
                amount=order_size,
                order_type=OrderType.Limit,
                src_type="test",
                src_timestamp=datetime.datetime.now(datetime.timezone.utc),
                time_in_force="FAS",  # Fill and Store
            )
            self._order_sent = True
            self._pending_order = order
            self.logger.info(f"Order sent: order_id={order.order_id}, status={order.order_status}")

            # Schedule order cancellation after 5 seconds
            asyncio.create_task(self._cancel_order_after_delay(order.order_id, delay=5))

        except Exception as e:
            self.logger.error(f"Failed to send order: {e}", exc_info=True)

    async def _handle_order(self, msg: Dict):
        """Handle Order status update."""
        order_id = msg.get("order_id")
        status = msg.get("order_status")
        self.logger.info(f"Order update: order_id={order_id}, status={status}")

        if self._pending_order and order_id == self._pending_order.order_id:
            if status in (OrderStatus.Filled, OrderStatus.Canceled):
                self.logger.info(f"Order completed: {status}")

    async def _handle_trade(self, msg: Dict):
        """Handle Trade execution notification."""
        self.logger.info(f"Trade executed: {msg}")

    async def _cancel_order_after_delay(self, order_id: str, delay: float = 5):
        """Cancel order after specified delay."""
        self.logger.info(f"Scheduling order cancellation in {delay} seconds...")
        await asyncio.sleep(delay)

        if self._order_canceled:
            return

        self.logger.info(f"Canceling order: order_id={order_id}")
        try:
            result = await self.client.cancel_order(
                order_id=order_id,
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                src_type="test",
                src_timestamp=datetime.datetime.now(datetime.timezone.utc),
                sym="BTCJPY",
            )
            self._order_canceled = True
            self.logger.info(f"Order canceled: {result}")

            # Schedule shutdown after 3 seconds
            asyncio.create_task(self._shutdown_after_delay(delay=3))

        except Exception as e:
            self.logger.error(f"Failed to cancel order: {e}", exc_info=True)
            # Still schedule shutdown even if cancel fails
            asyncio.create_task(self._shutdown_after_delay(delay=3))

    async def _shutdown_after_delay(self, delay: float = 3):
        """Request shutdown after specified delay."""
        self.logger.info(f"Scheduling shutdown in {delay} seconds...")
        await asyncio.sleep(delay)
        self._shutdown_requested = True
        self.logger.info("Shutdown requested")

    @property
    def should_shutdown(self) -> bool:
        return self._shutdown_requested


async def main():
    """
    Main test execution.

    Components:
    1. LocalWsPublisher: Internal message broker
    2. GmoCryptSocketClient: Public feed handler (Rate, MarketTrade)
    3. GmoPrivateStreamClient: Private stream (Order, Trade events)
    4. GmoRestClient: REST API client for order management
    5. LocalWsClient: Strategy message receiver
    6. GmoTestStrategy: Test strategy logic
    """
    print("=" * 60)
    print("GMO Private API Test Script")
    print("=" * 60)

    # Initialize components
    publisher = LocalWsPublisher(port=8766)  # Use different port to avoid conflicts

    # Public feed handler
    public_feed = GmoCryptSocketClient(
        ccy_pairs=["BTC_JPY"],
        publisher=publisher,
    )

    # Private stream client
    private_stream = GmoPrivateStreamClient(
        publisher=publisher,
    )

    # REST client
    rest_client = GmoRestClient(
        log_file="gmo_test.log",
    )

    # Strategy
    strategy = GmoTestStrategy(client=rest_client)

    # Local WebSocket client for strategy
    ws_client = LocalWsClient(
        uri="ws://localhost:8766",
        reconnect=False,
    )
    ws_client.register_callback(strategy.on_message)

    print("Starting components...")

    try:
        # Start publisher
        await publisher.start()
        print("Publisher started")
        await asyncio.sleep(1)

        # Start public feed
        asyncio.create_task(public_feed.start())
        print("Public feed started")
        await asyncio.sleep(1)

        # Start private stream
        asyncio.create_task(private_stream.start())
        print("Private stream started")
        await asyncio.sleep(1)

        # Connect WebSocket client
        await ws_client.connect()
        print("WebSocket client connected")

        # Wait for connection
        await asyncio.sleep(2)

        # Subscribe to Rate for BTC_JPY
        await ws_client.subscribe([
            {"_data_type": "Rate", "venue": "gmo", "sym": "BTCJPY"},
            {"_data_type": "Order", "venue": "gmo", "sym": "BTCJPY"},
            {"_data_type": "Trade", "venue": "gmo", "sym": "BTCJPY"},
            {"_data_type": "PositionUpdate", "venue": "gmo", "sym": "BTCJPY"},
        ])
        print("Subscribed to channels")
        print("-" * 60)
        print("Waiting for Rate message...")

        # Wait for shutdown signal
        timeout = 120  # Maximum wait time
        start_time = datetime.datetime.now()
        while not strategy.should_shutdown:
            await asyncio.sleep(0.5)
            elapsed = (datetime.datetime.now() - start_time).total_seconds()
            if elapsed > timeout:
                print(f"Timeout after {timeout} seconds")
                break

        print("-" * 60)
        print("Test completed!")

    except KeyboardInterrupt:
        print("\nInterrupted by user")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # Cleanup
        print("Cleaning up...")
        await ws_client.close()
        await public_feed.close()
        await private_stream.close()
        await publisher.close()
        await asyncio.sleep(1)
        print("All components stopped")


if __name__ == "__main__":
    asyncio.run(main())
