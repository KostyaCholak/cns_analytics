import asyncio
import logging
import sys
from datetime import timedelta, datetime
from typing import List, Dict

import aiohttp
import pytz

from cns_analytics.database import DataBase
from cns_analytics.entities import Resolution, MDType, Symbol, Exchange
from cns_analytics.market_data.base_loader import BaseMDLoader, MDLoaderException


logger = logging.getLogger(__name__)


class BarchartDailyLoader(BaseMDLoader):
    _session = None
    _authenticated = False
    source_id = 7

    def __init__(self):
        super().__init__()
        if BarchartDailyLoader._session is None or BarchartDailyLoader._session.closed:
            BarchartDailyLoader._authenticated = False
            BarchartDailyLoader._session = aiohttp.ClientSession(headers={
                "Referer": "https://www.barchart.com/futures/quotes/HG*0/interactive-chart",
                "sec-ch-ua": "\"Google Chrome\";v=\"93\", \" Not;A Brand\";v=\"99\", \"Chromium\";v=\"93\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"macOS\"",
                "upgrade-insecure-requests": "1",
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36'
            })

    async def get_supported_symbols(self, md_type) -> List[Symbol]:
        if not BarchartDailyLoader._authenticated:
            await self._rest_request(
                f"https://www.barchart.com/futures/quotes/CB*0/interactive-chart", {}, skip=True)
        return await super().get_supported_symbols(md_type)

    @staticmethod
    def get_step_for_resolution(md_type: MDType, resolution: Resolution) -> timedelta:
        if md_type in {MDType.MARKET_VOLUME, MDType.OHLC}:
            assert resolution is Resolution.d1
            return timedelta(days=365 * 40)

        raise NotImplementedError()

    async def _rest_request(self, url, params, is_retry=False, skip=False, headers=None):
        try:
            r = await self._session.get(url, params=params, headers=headers)
        except aiohttp.ClientOSError:
            if is_retry:
                raise

            await self._session.close()
            self._session = aiohttp.ClientSession()
            self.logger.exception("ClientOSError. Retrying...")
            await asyncio.sleep(1)
            return await self._rest_request(url, params, is_retry=True)

        if skip:
            token = self._session.cookie_jar.filter_cookies('https://www.barchart.com/')['XSRF-TOKEN'].value
            token = token.replace('%3D', '=')
            self._session.headers['x-xsrf-token'] = token
            self._session.headers['referrer'] = f'https://www.barchart.com/futures/quotes/ZB*0/interactive-chart'
            self._session.headers['sec-fetch-site'] = 'same-origin'
            self._session.headers['sec-fetch-mode'] = 'cors'
            self._session.headers['sec-fetch-dest'] = 'empty'
            assert r.ok
            return

        data = await r.text()
        lines = data.split('\n')

        assert r.ok

        parsed = []

        for line in lines:
            row = line.split(',')
            if len(row) < 7:
                continue
            parsed.append([
                row[1],
                float(row[2]),
                float(row[3]),
                float(row[4]),
                float(row[5]),
                float(row[6]),
            ])

        return parsed

    async def _fetch(self, symbol: Symbol, md_type: MDType,
                     start: datetime, end: datetime, resolution: Resolution) -> List[Dict]:

        if md_type is MDType.OHLC:
            raw_data = await self._rest_request(f"https://www.barchart.com/proxies/timeseries/queryeod.ashx", {
                'symbol': symbol.loader_external_name,
                'data': 'dailynearest' if symbol.loader_is_futures else 'daily',
                'start': start.strftime('%Y%m%d'),
                'end': end.strftime('%Y%m%d'),
                'volume': 'contract',
                'order': 'asc',
                'dividends': 'false',
                'backadjust': 'false',
                'daystoexpiration': '1',
                'contractroll': 'expiration',
            })

            data = []

            for ts, opn, high, low, cls, volume in raw_data:
                if opn is None:
                    continue

                ts = datetime.strptime(ts, '%Y-%m-%d').astimezone(tz=pytz.UTC)
                ts = ts.replace(hour=0, minute=0, second=0, microsecond=0)

                data.append({
                    "ts": ts,
                    "px_open": float(opn),
                    "px_high": float(high),
                    "px_low": float(low),
                    "px_close": float(cls),
                    "volume": float(volume)
                })

            await asyncio.sleep(0.5)

            return data

        raise NotImplementedError("Market Data Type is not implemented:", md_type.name)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()

    async def lookup_instruments(self, name):
        # ?q=al&fields=symbol,symbolName,exchange,symbolCode,symbolType,lastPrice,dailyLastPrice,hasOptions&meta=field.shortName,field.description&limit=50&searchType=contains&assetClasses=futures&regions=us&searchName=1&hasOptions=true&raw=1
        raw_data = await self._rest_request(
            f"https://www.barchart.com/proxies/core-api/v1/search", {
                'q': 'al',
                'fields': 'symbol,symbolName,exchange,symbolCode,symbolType,lastPrice,dailyLastPrice,hasOptions',
                'meta': 'field.shortName,field.description',
                'limit': 50,
                'searchType': 'contains',
                'assetClasses': 'futures',
                'region': '',
        })


async def main(symbols):
    aliases = {
        'T2': 'ZT',
        'T3': 'ZE',
        'T5': 'ZF',
        'T10': 'ZN',
        'T10U': 'TN',
        'T30': 'ZB',
        'T30U': 'UD',
        'RTS': '$RTS',
        'USDRUB': '^USDRUB',
        'EURUSD': '^EURUSD',
        'JPYUSD': '^JPYUSD',
        'RUBUSD': '^RUBUSD',
    }

    for symbol_name in symbols:
        name = aliases.get(symbol_name, symbol_name)

        symbol = Symbol(symbol_name, exchange=Exchange.BarchartDaily)
        symbol.loader_is_futures = len(name) <= 2
        symbol.loader_external_name = f'{name}*0' if symbol.loader_is_futures else name

        async with BarchartDailyLoader() as yfl:
            await yfl.fetch(
                symbol, md_type=MDType.OHLC, duration=timedelta(days=365 * 35), resolution=Resolution.d1)


if __name__ == '__main__':
    asyncio.run(main(sys.argv[1:]))
