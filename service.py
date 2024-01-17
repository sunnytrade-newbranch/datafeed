import logging
from cryptofeed.types import OrderBook

"""
数据结构请参考：https://github.com/bmoscon/orderbook
"""

logger = logging.getLogger("service")

async def orderbook(book, receipt_timestamp):
    logger.info(f'Book received at {receipt_timestamp} for {book.exchange} - {book.symbol}, with {len(book.book)} entries.')
    # logger.info(f'Top of book prices: {book.book.asks.index(0)[0]} - {book.book.bids.index(0)[0]}')
    # if book.delta:
    #     print(f"Delta from last book contains {len(book.delta[BID]) + len(book.delta[ASK])} entries.")