import token
import tokenize
from io import BytesIO

from cns_analytics import TimeSeries, DataBase, Exchange


async def _get_md(symbols, exchange):
    # TODO: do it with context manager
    DataBase.set_default_exchange(exchange)

    ts = TimeSeries(*symbols)
    await ts.load(resolution='1m')

    df = ts.get_raw_df()
    df['index'] = df.index

    return df


async def formula_to_ts(formula, exchange: Exchange = Exchange.Barchart) -> TimeSeries:
    context = {}

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
                if toknum not in context:
                    symbols.add(tokval)

                new_code.extend([
                    (token.NAME, 'df'),
                    (token.OP, '['),
                    (token.STRING, repr(tokval)),
                    (token.OP, ']')
                ])
        else:
            new_code.append((toknum, tokval))

    data = await _get_md(symbols, exchange)

    new_code_str = tokenize.untokenize(new_code).decode('utf-8')

    return TimeSeries.from_df(eval(new_code_str, context, {'df': data}).to_frame('SPREAD'))
