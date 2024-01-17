import time
import json
# from yapic import json
from multiprocessing import Process

from cryptofeed import FeedHandler
from cryptofeed.backends.zmq import BookZMQ, TickerZMQ
from cryptofeed.defines import L2_BOOK, TICKER
from cryptofeed.exchanges import Coinbase, Kraken, OKX


def load_symbol(file):
    symbols = []
    with open(file) as f:
        for line in f:
            symbol = line.strip()
            symbols.append(symbol)
    return symbols


def receiver(port):
    import zmq
    addr = 'tcp://127.0.0.1:{}'.format(port)
    ctx = zmq.Context.instance()
    s = ctx.socket(zmq.SUB)
    # empty subscription for all data, could be book for just book data, etc
    s.setsockopt(zmq.SUBSCRIBE, b'')

    s.connect(addr)
    while True:
        data = s.recv_string()
        key, msg = data.split(" ", 1)
        data = json.loads(msg)
        if "book" in key:
            local_timestamp = time.time()*1000
            print("feed传输延时", 1000*(data["receipt_timestamp"]-data["timestamp"]))
            print("本地处理耗时", local_timestamp - 1000*data["receipt_timestamp"])


def main():
    symbols = load_symbol("symbol.txt")
    print(symbols)
    try:
        p = Process(target=receiver, args=(5555,))

        p.start()

        f = FeedHandler()
        ok_symbols = symbols+[x+"-PERP" for x in symbols]
        f.add_feed(OKX(max_depth=5, channels=[L2_BOOK], symbols=ok_symbols, callbacks={L2_BOOK: BookZMQ(snapshots_only=True, snapshot_interval=2, port=5555)}))
        # f.add_feed(Coinbase(channels=[TICKER], symbols=['BTC-USD'], callbacks={TICKER: TickerZMQ(port=5678)}))

        f.run()

    finally:
        p.terminate()


if __name__ == '__main__':
    main()