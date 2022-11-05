import os
import asyncio
import logging

from datetime import datetime

import pytz
import aiohttp
import pandas as pd

from cns_analytics.entities import Symbol, MDType, Exchange
from cns_analytics.storage import Storage


logger = logging.getLogger(__name__)



class SimpleLoader:
    def __init__(self):
        pass
    
    async def load(self, symbol: Symbol, md_type: MDType):
        symbol.exchange = Exchange.YFinance
        logger.info(f"Loading {symbol.name}")
        data = await self._load(symbol, md_type)
        logger.info(f"Loaded {len(data)} data points")
        Storage.save_data(symbol, md_type, data)
        logger.info(f"Saved to database, done")
        return data
    

class YahooLoader(SimpleLoader):
    async def _rest_request(self, url, params, is_retry=False):
        async with aiohttp.ClientSession() as _session:
            try:
                r = await _session.get(url, params=params)
            except aiohttp.ClientOSError:
                await asyncio.sleep(1)
                return await self._rest_request(url, params, is_retry=True)

            data = await r.json()

            if data['chart']['error']:
                breakpoint()
                pass
                return [], []

            quote_data = []

            try:
                for key in ['low', 'open', 'volume', 'high', 'close']:
                    quote_data.append(data['chart']['result'][0]['indicators']['quote'][0][key])
            except KeyError:
                return [], []

            return data['chart']['result'][0]['timestamp'], \
                   list(zip(*quote_data))

    async def _load(self, symbol: Symbol, md_type: MDType):
        ts_data, ohlc_data = await self._rest_request(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol.name}", params={
            "formatted": "true",
            "crumb": "JAddEqUFuOp",
            "lang": "en-US",
            "region": "US",
            "includeAdjustedClose": "true",
            "interval": "1d",
            "period1": int(datetime(year=1970, month=1, day=1).timestamp()),
            "period2": int(datetime.now().timestamp()),
            "events": "div",
            "corsDomain": "finance.yahoo.com"
        })
        
        data = []

        for (ts, (low, opn, volume, high, cls)) in zip(ts_data, ohlc_data):
            if opn is None:
                continue

            ts = datetime.fromtimestamp(ts).astimezone(tz=pytz.UTC)
            ts = ts.replace(hour=0, minute=0, second=0, microsecond=0)

            data.append({
                "ts": ts,
                "px_open": float(opn),
                "px_high": float(high),
                "px_low": float(low),
                "px_close": float(cls),
                "volume": float(volume)
            })
        
        
        data = pd.DataFrame(data)
        data = data.set_index('ts')
        return data


async def main():
    loader = YahooLoader()
    await loader.load(Symbol("GCIIX"), MDType.OHLC)


if __name__ == '__main__':
    asyncio.run(main())
