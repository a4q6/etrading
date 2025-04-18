import asyncio
from etr.core.feed_handler.bitbank import BitBankSocketClient

if __name__ == '__main__':
    client = BitBankSocketClient(
        ccy_pairs=["btc_jpy", "xrp_jpy", "eth_jpy", "doge_jpy", "bcc_jpy", "ltc_jpy"]
    )
    try:
        asyncio.run(client.start())
    except KeyboardInterrupt:
        print("Disconnected")
        asyncio.gather(client.close())
