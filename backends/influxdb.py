'''
Copyright (C) 2017-2024 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from collections import defaultdict
import logging

from yapic import json

from cryptofeed.backends.backend import BackendBookCallback, BackendCallback
from cryptofeed.backends.http import HTTPCallback
from cryptofeed.defines import BID, ASK

LOG = logging.getLogger('feedhandler')


class InfluxCallback(HTTPCallback):
    def __init__(self, addr: str, db: str, key=None, **kwargs):
        """
        Parent class for InfluxDB callbacks

        influxDB schema
        ---------------
        MEASUREMENT | TAGS | FIELDS

        Measurement: Data Feed-Exchange (configurable)
        TAGS: symbol
        FIELDS: timestamp, amount, price, other funding specific fields

        Example data in InfluxDB
        ------------------------
        > select * from "book-COINBASE";
        name: COINBASE
        time                amount    symbol    price   side timestamp
        ----                ------    ----    -----   ---- ---------
        1542577584985404000 0.0018    BTC-USD 5536.17 bid  2018-11-18T21:46:24.963762Z
        1542577584985404000 0.0015    BTC-USD 5542    ask  2018-11-18T21:46:24.963762Z
        1542577585259616000 0.0018    BTC-USD 5536.17 bid  2018-11-18T21:46:25.256391Z

        Parameters
        ----------
        addr: str
          Address for connection. Should be in the format:
          http(s)://<ip addr>:port
        org: str
          Organization name for authentication
        bucket: str
          Bucket name for authentication
        token: str
          Token string for authentication
        key:
          key to use when writing data, will be a combination of key-datatype
        """
        super().__init__(addr, **kwargs)
        self.addr = f"{addr}/api/write?db={db}"

        self.session = None
        self.key = key if key else self.default_key
        self.numeric_type = float
        self.none_to = None
        self.running = True

    def format(self, data):
        ret = []
        for key, value in data.items():
            if key in {'timestamp', 'exchange', 'symbol', 'receipt_timestamp'}:
                continue
            if isinstance(value, str) or value is None:
                ret.append(f'{key}="{value}"')
            else:
                ret.append(f'{key}={value}')
        return ','.join(ret)

    async def writer(self):
        while self.running:
            async with self.read_queue() as updates:
                for update in updates:
                    d = self.format(update)
                    timestamp = update["timestamp"]
                    timestamp_str = f',timestamp={timestamp}' if timestamp is not None else ''

                    if 'interval' in update:
                        trades = f',trades={update["trades"]},' if update['trades'] else ','
                        update = f'{self.key}-{update["exchange"]},symbol={update["symbol"]},interval={update["interval"]} start={update["start"]},stop={update["stop"]}{trades}open={update["open"]},close={update["close"]},high={update["high"]},low={update["low"]},volume={update["volume"]}{timestamp_str},receipt_timestamp={update["receipt_timestamp"]} {int(update["receipt_timestamp"] * 1000000)}'
                    else:
                        update = f'{self.key}-{update["exchange"]},symbol={update["symbol"]} {d}{timestamp_str},receipt_timestamp={update["receipt_timestamp"]} {int(update["receipt_timestamp"] * 1000000)}'

                    await self.http_write(update, headers=self.headers)
        await self.session.close()


class TradeInflux(InfluxCallback, BackendCallback):
    default_key = 'trades'

    def format(self, data):
        return f'side="{data["side"]}",price={data["price"]},amount={data["amount"]},id="{str(data["id"])}",type="{str(data["type"])}"'


class FundingInflux(InfluxCallback, BackendCallback):
    default_key = 'funding'


class BookInflux(InfluxCallback, BackendBookCallback):
    default_key = 'book'

    def __init__(self, *args, snapshots_only=False, snapshot_interval=1000, **kwargs):
        self.snapshots_only = snapshots_only
        self.snapshot_interval = snapshot_interval
        self.snapshot_count = defaultdict(int)
        super().__init__(*args, **kwargs)

    def format(self, data):
        delta = 'delta' in data
        book = data['book'] if not delta else data['delta']
        bids = json.dumps(book[BID])
        asks = json.dumps(book[ASK])

        return f'delta={str(delta)},{BID}="{bids}",{ASK}="{asks}"'


class TickerInflux(InfluxCallback, BackendCallback):
    default_key = 'ticker'


class OpenInterestInflux(InfluxCallback, BackendCallback):
    default_key = 'open_interest'


class LiquidationsInflux(InfluxCallback, BackendCallback):
    default_key = 'liquidations'


class CandlesInflux(InfluxCallback, BackendCallback):
    default_key = 'candles'


class OrderInfoInflux(InfluxCallback, BackendCallback):
    default_key = 'order_info'


class TransactionsInflux(InfluxCallback, BackendCallback):
    default_key = 'transactions'


class BalancesInflux(InfluxCallback, BackendCallback):
    default_key = 'balances'


class FillsInflux(InfluxCallback, BackendCallback):
    default_key = 'fills'