from typing import Optional, List, Union

from cns_analytics import DateTime, Symbol, TimeSeries
from cns_analytics.entities import DropLogic


async def find_rising_spreads(
     rising_symbols: List[Union[Symbol, str]],
     hedging_symbols: List[Union[Symbol, str]],
     max_symbol_num: int = 2,
     start: Optional[DateTime] = None,
     end: Optional[DateTime] = None
):
    results = []

    expected_symbols_len = 2

    for rising in rising_symbols:
        for hedge in hedging_symbols:
            symbols = rising, hedge

            if len(list(set(symbols))) != expected_symbols_len:
                continue

            ts = TimeSeries(*symbols)
            await ts.load(start=start, end=end)
            if ts.empty():
                continue

            ts.scale_to(100, symbol=symbols[0])
            ts, _ = ts.optimize.drop()
            max_drop = ts.get_drop(logic=DropLogic.SIMPLE).max()

            if max_drop < 10:
                print(symbols)
                ts.plot()
                results.append((symbols, max_drop))

    results.sort(key=lambda x: x[-1])

    return results
