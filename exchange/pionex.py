import asyncio
import time
import logging
from collections import defaultdict
from decimal import Decimal
from typing import Dict, Tuple

from yapic import json
# import json

from cryptofeed.connection import AsyncConnection, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.feed import Feed
from cryptofeed.exchange import RestExchange
from cryptofeed.defines import CALL, CANCELLED, FILL_OR_KILL, FUTURES, IMMEDIATE_OR_CANCEL, MAKER_OR_CANCEL, MARKET, OKX as OKX_str, LIQUIDATIONS, BUY, OPEN, OPTION, PARTIAL, PERPETUAL, PUT, SELL, FILLED, ASK, BID, FUNDING, L2_BOOK, OPEN_INTEREST, TICKER, TRADES, ORDER_INFO, CANDLES, SPOT, UNFILLED, LIMIT
from cryptofeed.symbols import Symbol
from cryptofeed.exceptions import BadChecksum
from cryptofeed.types import OrderBook


class PionexRestMixin(RestExchange):
    api = "https://api.pionex.com"


LOG = logging.getLogger("feedhandler")


class Pionex(Feed, PionexRestMixin):
    id = "pionex_spot"
    valid_candle_intervals = {'1M', '1W', '1D', '12H', '6H', '4H', '2H', '1H', '30m', '15m', '5m', '3m', '1m'}
    candle_interval_map = {'1M': 2630000, '1W': 604800, '1D': 86400, '12H': 43200, '6H': 21600, '4H': 14400, '2H': 7200, '1H': 3600, '30m': 1800, '15m': 900, '5m': 300, '3m': 180, '1m': 60}
    websocket_channels = {
        L2_BOOK: 'books',
        TRADES: 'trades',
        TICKER: 'tickers',
        FUNDING: 'funding-rate',
        OPEN_INTEREST: 'open-interest',
        LIQUIDATIONS: LIQUIDATIONS,
        ORDER_INFO: 'orders',
        CANDLES: 'candle'
    }
    websocket_endpoints = [
        WebsocketEndpoint('wss://ws.pionex.com/wsPub', channel_filter=(websocket_channels[L2_BOOK], websocket_channels[TRADES], websocket_channels[TICKER], websocket_channels[FUNDING], websocket_channels[OPEN_INTEREST], websocket_channels[LIQUIDATIONS], websocket_channels[CANDLES]), options={'compression': None}),
        WebsocketEndpoint('wss://ws.pionex.com/ws', channel_filter=(websocket_channels[ORDER_INFO],), options={'compression': None}),
    ]
    rest_endpoints = [RestEndpoint('https://api.pionex.com', routes=Routes(['/api/v1/common/symbols']))]
    request_limit = 20

    @classmethod
    def timestamp_normalize(cls, ts: float) -> float:
        return ts / 1000.0


    @classmethod
    def _parse_symbol_data(cls, data: list) -> Tuple[Dict, Dict]:
        ret = {}
        info = defaultdict(dict)

        symbols = data.get("data").get("symbols")
        for e in symbols:
            expiry = None
            otype = None
            stype = e["type"].lower()
            strike = None
            
            base = e['baseCurrency']
            quote = e['quoteCurrency']

            s = Symbol(base, quote, expiry_date=expiry, type=stype, option_type=otype, strike_price=strike)
            ret[s.normalized] = e['symbol']
            info['tick_size'][s.normalized] = e['minTradeSize']
            info['instrument_type'][s.normalized] = stype

        return ret, info
    
    def __reset(self):
        self._l2_book = {}

    async def subscribe(self, connection: AsyncConnection):
        channels = []
        for chan in self.subscription:
            for pair in self.subscription[chan]:
                msg = self.build_subscription(chan, pair)
                await asyncio.sleep(0.4)
                await connection.write(json.dumps(msg))
    
    def build_subscription(self, channel: str, ticker: str) -> dict:
        if channel in ['books']:
            subscription_dict = {
                "op": "SUBSCRIBE",
                "topic": 'DEPTH',
                "symbol": ticker,
                "limit":  10
            }
        else:
            subscription_dict = {}
            
        return subscription_dict
    
    async def _book(self, msg: dict, timestamp: float):
        # snapshot
        pair = self.exchange_symbol_to_std_symbol(msg['symbol'])

        bids = {Decimal(price): Decimal(amount) for price, amount, *_ in msg['data']['bids']}
        asks = {Decimal(price): Decimal(amount) for price, amount, *_ in msg['data']['asks']}
        self._l2_book[pair] = OrderBook(self.id, pair, max_depth=self.max_depth, bids=bids, asks=asks)
        
        await self.book_callback(L2_BOOK, self._l2_book[pair], timestamp, timestamp=self.timestamp_normalize(int(msg['timestamp'])), raw=msg)

    async def message_handler(self, msg: str, connection: AsyncConnection, timestamp: float):
        # DEFLATE compression, no header
        # msg = zlib.decompress(msg, -15)
        # not required, as websocket now set to "Per-Message Deflate"
        msg = json.loads(msg, parse_float=Decimal)

        op = msg.get("op", None)
        if op:
            # 处理派网的ping
            if op == "PING":
                pong = {
                    "op": "PONG",
                    "timestamp": int(100*time.time())
                }
                await connection.write(json.dumps(pong))
                LOG.info(pong)
            return

        # 行情推送
        type = msg.get("type", None)
        if type:
            LOG.info("type {} {}".format(type, msg))
            return

        topic = msg.get("topic", None)
        if topic:
            if topic == "DEPTH":
                await self._book(msg, timestamp)
            else:
                LOG.warning("%s: Unknow topic {} {}".format(self.id, topic, msg))
        else:
            LOG.warning("%s: Unhandled message %s", self.id, msg)