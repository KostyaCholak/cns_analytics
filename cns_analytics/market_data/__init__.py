from datetime import timedelta
from cns_analytics.market_data.barchart_daily import BarchartDailyLoader
from cns_analytics.market_data.barchart_intraday import BarchartIntradayLoader
from cns_analytics.market_data.fred_loader import FredLoader
from cns_analytics.entities import Symbol, Exchange, MDType, Resolution

async def download_data(symbol_name, exchange, duration=None):
    symbol = Symbol(symbol_name, exchange)
    
    if exchange is Exchange.BarchartDaily:
        duration = duration or timedelta(days=365*10)
        async with BarchartDailyLoader() as loader:
            fut = len(symbol_name) <= 2
            symbol.loader_is_futures = fut

            symbol.loader_external_name = f"{symbol_name}*0" if fut else symbol_name
            await loader.fetch(symbol, md_type=MDType.OHLC, duration=duration, resolution=Resolution.d1)
    elif exchange is Exchange.Barchart:
        duration = duration or timedelta(days=365)
        async with BarchartIntradayLoader() as loader:
            fut = len(symbol_name) <= 2
            symbol.loader_is_futures = fut

            symbol.loader_external_name = f"{symbol_name}*0" if fut else symbol_name
            await loader.fetch(symbol, md_type=MDType.OHLC, duration=duration, resolution=Resolution.m1)
    elif exchange is Exchange.Fred:
        await FredLoader().load(symbol, MDType.OHLC)

