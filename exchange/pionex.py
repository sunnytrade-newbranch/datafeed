import asyncio
from decimal import Decimal
import logging

# from yapic import json
import json

from cryptofeed.connection import AsyncConnection, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.feed import Feed
from cryptofeed.exchange import RestExchange
from cryptofeed.defines import CALL, CANCELLED, FILL_OR_KILL, FUTURES, IMMEDIATE_OR_CANCEL, MAKER_OR_CANCEL, MARKET, OKX as OKX_str, LIQUIDATIONS, BUY, OPEN, OPTION, PARTIAL, PERPETUAL, PUT, SELL, FILLED, ASK, BID, FUNDING, L2_BOOK, OPEN_INTEREST, TICKER, TRADES, ORDER_INFO, CANDLES, SPOT, UNFILLED, LIMIT


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
    rest_endpoints = [RestEndpoint('https://www.okx.com', routes=Routes(['/api/v5/public/instruments?instType=SPOT', '/api/v5/public/instruments?instType=SWAP', '/api/v5/public/instruments?instType=FUTURES', '/api/v5/public/instruments?instType=OPTION&uly=BTC-USD', '/api/v5/public/instruments?instType=OPTION&uly=ETH-USD'], liquidations='/api/v5/public/liquidation-orders?instType={}&limit=100&state={}&uly={}'))]
    request_limit = 20

    @classmethod
    def timestamp_normalize(cls, ts: float) -> float:
        return ts / 1000.0
    
    def __reset(self):
        self._l2_book = {}

    async def subscribe(self, connection: AsyncConnection):
        channels = []
        for chan in self.subscription:
            if chan == LIQUIDATIONS:
                asyncio.create_task(self._liquidations(self.subscription[chan]))
                continue
            for pair in self.subscription[chan]:
                channels.append(self.build_subscription(chan, pair))

            msg = {"op": "subscribe", "args": channels}
            await connection.write(json.dumps(msg))