import time
import json
import logging

from cryptofeed import FeedHandler
from cryptofeed.backends.zmq import BookZMQ, TickerZMQ
from cryptofeed.defines import L2_BOOK, BID, ASK
from cryptofeed.exchanges import Coinbase, Kraken, OKX

import config
from exchange import Pionex
from service import orderbook

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s: %(message)s")
logger = logging.getLogger("main")

def get_symbols():
    symbols = []
    for symbol in config.symbols:
        n_symbol = symbol.replace("_", "-")
        symbols.append(n_symbol)
    return symbols

def main():
    symbols = get_symbols()

    f = FeedHandler()
    ok_swap_symbols = symbols+[x+"-PERP" for x in symbols]
    f.add_feed(OKX(max_depth=10, channels=[L2_BOOK], symbols=symbols, callbacks={L2_BOOK: orderbook}))
    # f.add_feed(Coinbase(channels=[TICKER], symbols=['BTC-USD'], callbacks={TICKER: TickerZMQ(port=5678)}))
    f.add_feed(Pionex(max_depth=10, channels=[L2_BOOK], symbols=symbols, callbacks={L2_BOOK: orderbook}))

    f.run()


if __name__ == '__main__':
    main()