import asyncio
import logging
import sys
from datetime import timedelta, datetime
from typing import List, Dict

import aiohttp
import pytz
import pandas as pd
from finam import Exporter, Market, LookupComparator, Timeframe

from cns_analytics.entities import Resolution, MDType, Symbol, Exchange
from cns_analytics.market_data.base_loader import BaseMDLoader, MDLoaderException


logger = logging.getLogger(__name__)


class FinamTicksLoader(BaseMDLoader):
    source_id = 6
    stop_on_empty = False

    def __init__(self):
        super().__init__()
        self._session = aiohttp.ClientSession()
        self.exporter = Exporter()
        self._sym_cache = {}

    def find_symbol(self, symbol: Symbol, market: Market):
        if symbol.name in self._sym_cache:
            return self._sym_cache[symbol.name]

        replace_map = {
            'USDRUB_TOM': 'USD000UTSTOM',
            'EURRUB_TOM': 'EUR_RUB__TOM',
        }

        code = replace_map.get(symbol.name, symbol.name)

        sym = self.exporter.lookup(code=code, market=market,
                                   name_comparator=LookupComparator.CONTAINS)
        assert len(sym) == 1

        self._sym_cache[symbol.name] = sym.index[0]

        return sym.index[0]

    @staticmethod
    def get_step_for_resolution(md_type: MDType, resolution: Resolution) -> timedelta:
        if md_type in {MDType.MARKET_VOLUME, MDType.OHLC, MDType.TICKS}:
            if resolution is Resolution.m1:
                return timedelta(days=1)
        raise NotImplementedError()

    async def _fetch(self, symbol: Symbol, md_type: MDType,
                     start: datetime, end: datetime, resolution: Resolution) -> List[Dict]:
        if resolution is Resolution.m1:
            tf = Timeframe.TICKS
        else:
            raise NotImplementedError()

        market = Market.FUTURES
        if 'RUB' in symbol.name:
            market = Market.CURRENCIES

        if md_type is MDType.TICKS:
            df = self.exporter.download(self.find_symbol(symbol, market),
                                        market=market,
                                        timeframe=tf,
                                        start_date=start,
                                        end_date=end)

            breakpoint()
            df['ts'] = pd.to_datetime(df['<DATE>'].astype(str) + ' ' + df['<TIME>'])
            df.drop(columns=['<DATE>', '<TIME>', '<TICKER>', '<PER>'], inplace=True)

            data = []
            ts_shift = 1
            last_ts = None

            for last, volume, ts in df.values:
                ts = ts.to_pydatetime().astimezone(tz=pytz.UTC)
                if last_ts == ts:
                    last_ts = ts
                    ts = ts.replace(microsecond=ts_shift)
                    ts_shift += 1
                else:
                    last_ts = ts
                    ts_shift = 1

                if last is None:
                    continue
                data.append({
                    "ts": ts,
                    "px_open": float(last),
                    "px_high": float(last),
                    "px_low": float(last),
                    "px_close": float(last),
                    "volume": float(volume or 0)
                })

            return data


        if md_type is MDType.OHLC:
            df = self.exporter.download(self.find_symbol(symbol, market),
                                        market=market,
                                        timeframe=tf,
                                        start_date=start,
                                        end_date=end)

            breakpoint()
            df['ts'] = pd.to_datetime(df['<DATE>'].astype(str) + ' ' + df['<TIME>'])
            df.drop(columns=['<DATE>', '<TIME>', '<TICKER>', '<PER>'], inplace=True)

            data = []
            ts_shift = 1
            last_ts = None

            for last, volume, ts in df.values:
                ts = ts.to_pydatetime().astimezone(tz=pytz.UTC)
                if last_ts == ts:
                    last_ts = ts
                    ts = ts.replace(microsecond=ts_shift)
                    ts_shift += 1
                else:
                    last_ts = ts
                    ts_shift = 1

                if last is None:
                    continue
                data.append({
                    "ts": ts,
                    "px_open": float(last),
                    "px_high": float(last),
                    "px_low": float(last),
                    "px_close": float(last),
                    "volume": float(volume or 0)
                })

            return data

        raise NotImplementedError("Market Data Type is not implemented:", md_type.name)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()


async def main():
    name = sys.argv[1]
    symbol = Symbol(name, exchange=Exchange.FinamTicks)

    async with FinamTicksLoader() as fnm:
        await fnm.fetch(
            symbol, md_type=MDType.TICKS, duration=timedelta(days=int(sys.argv[2])), resolution=Resolution.m1)


if __name__ == '__main__':
    asyncio.run(main())
