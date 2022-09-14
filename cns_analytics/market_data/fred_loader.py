import asyncio
import logging

import pytz

from cns_analytics.entities import Symbol, MDType
from cns_analytics.storage import Storage

from fredapi import Fred


logger = logging.getLogger(__name__)



class SimpleLoader:
    def __init__(self):
        pass
    
    async def load(self, symbol: Symbol, md_type: MDType):
        logger.info(f"Loading {symbol.name}")
        data = await self._load(symbol, md_type)
        logger.info(f"Loaded {len(data)} data points")
        Storage.save_data(symbol, md_type, data)
        logger.info(f"Saved to database, done")
        return data
    

class FredLoader(SimpleLoader):
    def __init__(self):
        super().__init__()
        self.api_key = os.getenv('FRED_API_KEY')

    async def _load(self, symbol: Symbol, md_type: MDType):
        fred = Fred(api_key=self.api_key)
        data = fred.get_series(symbol.name)
        if md_type is MDType.EOD:
            data = data.to_frame("value")
        else:
            data = data.to_frame("px_close")
            data['px_open'] = data['px_close']
            data['px_high'] = data['px_close']
            data['px_low'] = data['px_close']
            data['volume'] = 0
        data.index = data.index.tz_localize(pytz.UTC)
        data.index.name = 'ts'
        return data


async def main():
    loader = FredLoader()
    loader.load("T5YIE")


if __name__ == '__main__':
    asyncio.run(main())
