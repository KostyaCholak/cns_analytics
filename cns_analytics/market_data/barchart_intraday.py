import asyncio
import logging
import sys
import numpy as np
from datetime import timedelta, datetime
from typing import List, Dict

import aiohttp
import pytz

from cns_analytics.database import DataBase
from cns_analytics.entities import Resolution, MDType, Symbol, Exchange
from cns_analytics.market_data.base_loader import BaseMDLoader, RequestedTooMuchData


logger = logging.getLogger(__name__)


class BarchartIntradayLoader(BaseMDLoader):
    _session = None
    _authenticated = False
    source_id = 3
    stop_on_empty = False

    def __init__(self):
        super().__init__()
        self._data_points_per_call = []
        self._last_mult = None
        if BarchartIntradayLoader._session is None or BarchartIntradayLoader._session.closed:
            BarchartIntradayLoader._authenticated = False
            BarchartIntradayLoader._session = aiohttp.ClientSession(headers={
#                 "Referer": "https://www.barchart.com/futures/quotes/HG*0/interactive-chart",
                "sec-ch-ua": "\"Google Chrome\";v=\"93\", \" Not;A Brand\";v=\"99\", \"Chromium\";v=\"93\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"macOS\"",
                "upgrade-insecure-requests": "1",
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36'
            })

    async def get_supported_symbols(self, md_type) -> List[Symbol]:
        if not BarchartIntradayLoader._authenticated:
            await self._rest_request(
                f"https://www.barchart.com/futures/quotes/HG*0/interactive-chart", {}, skip=True)
        return await super().get_supported_symbols(md_type)

#     @staticmethod
    def get_step_for_resolution(self, md_type: MDType, resolution: Resolution) -> timedelta:
        if md_type in {MDType.MARKET_VOLUME, MDType.OHLC}:
            if len(self._data_points_per_call) > 5:
                mult = 7000 / (max(self._data_points_per_call) + 1)
                
                mult = self._last_mult * mult
            else:
                mult = 1
            mult = min(max(mult, 1), 5)
            self._last_mult = mult
            assert resolution is Resolution.m1
            return timedelta(days=round(7 * 1 * mult))

        raise NotImplementedError()

    async def _rest_request(self, url, params, is_retry=False, skip=False, headers=None):
        await asyncio.sleep(0.05)
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
            self._session.headers['referrer'] = f'https://www.barchart.com/futures/quotes/HG*0/interactive-chart'
            self._session.headers['sec-fetch-site'] = 'same-origin'
            self._session.headers['sec-fetch-mode'] = 'cors'
            self._session.headers['sec-fetch-dest'] = 'empty'
            assert r.ok
            return
#         print(r)
        assert r.ok

        data = await r.json()
        parsed = []
        
        data_points_num = len(data['data'])

        if data_points_num >= 9998:
            self._data_points_per_call.clear()
            raise RequestedTooMuchData()
        
        self._data_points_per_call.append(data_points_num)
        self._data_points_per_call = self._data_points_per_call[-10:]

        if 'data' not in data:
            breakpoint()

        for row in data['data']:
            parsed.append([
                row['raw']['tradeTime'],
                row['raw']['openPrice'],
                row['raw']['highPrice'],
                row['raw']['lowPrice'],
                row['raw']['lastPrice'],
                row['raw']['volume'],
            ])

        return parsed

    async def _fetch(self, symbol: Symbol, md_type: MDType,
                     start: datetime, end: datetime, resolution: Resolution) -> List[Dict]:
        if md_type is MDType.OHLC:
            raw_data = await self._rest_request(f"https://www.barchart.com/proxies/core-api/v1/historical/get", {
                'symbol': symbol.loader_external_name,
                'fields': 'symbol,tradeTime.format(m/d/Y),openPrice,highPrice,lowPrice,lastPrice,priceChange,percentChange,volume,symbolCode,symbolType',
                'type': 'nearby_minutes' if symbol.loader_is_futures else 'minutes',
                'orderBy': 'tradeTime',
                'orderDir': 'desc',
                'limit': '9999',
                'meta': 'field.shortName,field.type,field.description',
                'startDate': start.strftime('%Y-%m-%d'),
                'page': '1',
                'interval': '1',
                'contractRoll': 'combined',
                'endDate': end.strftime('%Y-%m-%d'),
                'raw': '1',
            })

            data = []

            for ts, opn, high, low, cls, volume in raw_data:
                if opn is None:
                    continue

                ts = datetime.strptime(ts, '%Y-%m-%d %H:%M').astimezone(tz=pytz.UTC)
                ts = ts.replace(second=0, microsecond=0)

                data.append({
                    "ts": ts,
                    "px_open": float(opn),
                    "px_high": float(high),
                    "px_low": float(low),
                    "px_close": float(cls),
                    "volume": float(volume)
                })

            await asyncio.sleep(0.3)

            return data

        raise NotImplementedError("Market Data Type is not implemented:", md_type.name)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()


EXTERNAL_SYMBOL = None
FUTURES = True

ALIASES = {
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


async def main():
    global EXTERNAL_SYMBOL, FUTURES
    # for symbol in (await DataBase.get_all_symbols(Exchange.YFinance)):
    #     await DataBase.drop_symbol(symbol)

    # T2 = ZT
    # T3 = ZE
    # T5 = ZF
    # T10 = ZN
    # T10U = TN
    # T30 = ZB
    # T30U = UD

    # SLY - small cap eth

    symbol_names = [
        'RSX',
        'EWU',
        'TUR',
        'THD',
        'FLTW',
        'EWL',
        'EWD',
        'EWP',
        'EWY',
        'EZA',
        'EWS',
        'KSA',
        'ERUS',
        'EPOL',
        'EPHE',
        'EPU',
        'ENZL',
        'EWN',
        'EWM',
        'FLJP',
        'EWI',
        'EIS',
        'EIS',
        'KWT',
        'EIRL',
        'EIDO',
        'INDA',
        'FLHK',
        'NORW',
        'GREK',
        'EWG',
        'EWQ',
        'EFNL',
        'EDEN',
        'FLCH',
        'ECH',
        'EWZ',
        'EWK',
        'EWO',
        'FLAU',
        'AGT',
    ]


    for symbol_name in symbol_names:
        # symbol = 'XLI'
        FUTURES = False
        name = ALIASES.get(symbol_name, symbol_name)

        symbol = await DataBase.create_symbol(symbol_name, exchange=Exchange.Barchart)
        EXTERNAL_SYMBOL = f'{name}Z21' if FUTURES else name

        async with BarchartIntradayLoader() as loader:
            await loader.fetch(
                symbol, md_type=MDType.OHLC, duration=timedelta(days=365 * 2), resolution=Resolution.m1)


async def update_all():
    global EXTERNAL_SYMBOL, FUTURES

    non_futures = [
        'RSX',
        'EWU',
        'TUR',
        'THD',
        'FLTW',
        'EWL',
        'EWD',
        'EWP',
        'EWY',
        'EZA',
        'EWS',
        'KSA',
        'ERUS',
        'EPOL',
        'EPHE',
        'EPU',
        'ENZL',
        'EWN',
        'EWM',
        'FLJP',
        'EWI',
        'EIS',
        'EIS',
        'KWT',
        'EIRL',
        'EIDO',
        'INDA',
        'FLHK',
        'NORW',
        'GREK',
        'EWG',
        'EWQ',
        'EFNL',
        'EDEN',
        'FLCH',
        'ECH',
        'EWZ',
        'EWK',
        'EWO',
        'FLAU',
        'AGT',
    ]
    futures = [
    ]

    async with BarchartIntradayLoader() as loader:
        FUTURES = True

        for name in futures:
            symbol = Symbol(name, Exchange.Barchart)
            name = ALIASES.get(name, name)
            EXTERNAL_SYMBOL = f"{name}Z21"
            await loader.fetch(
                symbol, md_type=MDType.OHLC, duration=timedelta(days=90), resolution=Resolution.m1)

        FUTURES = False

        for name in non_futures:
            symbol = Symbol(name, Exchange.Barchart)
            EXTERNAL_SYMBOL = ALIASES.get(name, name)
            await loader.fetch(
                symbol, md_type=MDType.OHLC, duration=timedelta(days=365 * 2), resolution=Resolution.m1)


if __name__ == '__main__':
    try:
        arg = sys.argv[1]
    except IndexError:
        arg = ''

    if arg == 'all':
        asyncio.run(update_all())
    else:
        asyncio.run(main())
