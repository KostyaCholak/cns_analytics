import token
import tokenize
from io import BytesIO

import numpy as np
import pandas as pd
from cns_analytics import TimeSeries, DataBase, Exchange, Symbol
from cns_analytics.market_data import download_data
from cns_analytics.ta import sma, rsi


async def _get_md(symbols, exchange, start=None, end=None):
    _symbols = []
    for symbol in symbols:
        if isinstance(symbol, Symbol):
            _symbols.append(symbol)
        else:
            _symbols.append(Symbol(symbol, exchange))

    ts = TimeSeries(*_symbols)
    try:
        await ts.load(start=start, end=end)
    except KeyError:
        for symbol in _symbols:
            await download_data(symbol.name, symbol.exchange)
        await ts.load(start=start, end=end)

    df = ts.get_raw_df()
    df['index'] = df.index
    
    return df


def fix_money(series, money=100, interval=pd.Timedelta('90d')):
    open_money = 0
    qty = 0
    last_rebalance = None
    hist = []

    for ts, px in series.items():
        if last_rebalance is None or ts - last_rebalance > interval:
            open_money += qty * px
            qty = money / px
            open_money -= qty * px
            last_rebalance = ts
        hist.append(open_money + qty * px)
    
    return pd.Series(hist, name=series.name, index=series.index) + money


def mask_formula(df, formula) -> pd.Series:
    context = {
        'm': fix_money,
        'sma': sma,
        'rsi': rsi,
        'np': np,
        'pd': pd,
    }
    return eval(formula, context, {'df': df})


async def formula(formula, exchange: Exchange = Exchange.BarchartDaily, start=None, end=None) -> pd.DataFrame:
    context = {
        'm': fix_money,
        'sma': sma,
        'np': np,
        'pd': pd,
    }

    code = list(tokenize.tokenize(BytesIO(formula.encode('utf-8')).readline))
    new_code = []

    symbols = set()

    for idx, (toknum, tokval, _, _, _) in enumerate(code):
        if toknum == token.NAME:
            if tokval == 'df':
                symbol = code[idx + 2][1][1:-1]
                symbols.add(symbol)
                new_code.append((toknum, tokval))
            else:
                if tokval not in context:
                    symbols.add(tokval)

                    new_code.extend([
                        (token.NAME, 'df'),
                        (token.OP, '['),
                        (token.STRING, repr(tokval)),
                        (token.OP, ']')
                    ])
                else:
                    new_code.append((toknum, tokval))
        else:
            new_code.append((toknum, tokval))

    data = await _get_md(symbols, exchange, start, end)

    new_code_str = tokenize.untokenize(new_code).decode('utf-8')

    return eval(new_code_str, context, {'df': data}).to_frame('SPREAD')


async def formula_to_ts(formula, exchange: Exchange = Exchange.BarchartDaily, start=None, end=None) -> TimeSeries:
    df = await formula(formula, exchange, start, end)
    return TimeSeries.from_df(df)