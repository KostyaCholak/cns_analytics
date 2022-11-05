import sys
import asyncio
import pytz
import pandas as pd

from cns_analytics.storage import Storage
from cns_analytics.entities import Resolution, MDType, Symbol, Exchange


async def main():
    filename = sys.argv[1]
    name = sys.argv[2]
    try:
        reset = sys.argv[3] in {'1', 'yes', '+', 'y'}
    except IndexError:
        reset = False
    md_type = MDType.TICKS
    symbol = Symbol(name, exchange=Exchange.FinamTicks)

    df = pd.read_csv(filename)
    df['ts'] = pd.to_datetime(df['<DATE>'].astype(str) + ' ' + df['<TIME>'].astype(str), format='%Y%m%d %H%M%S')
    df.drop(columns=['<DATE>', '<TIME>', '<ID>'], inplace=True)
    df = df.set_index('ts')
    df.index = df.index.tz_localize(pytz.UTC)
    df.index.names = ['ts']
    df.columns = ['px', 'qty', 'maker_side']

    try:
        if reset:
            raise KeyError
        full_df = Storage.load_data(symbol, md_type)
        full_df = pd.concat([full_df, df])
        full_df = full_df.sort_index()
        # full_df = full_df[~full_df.index.duplicated(keep='first')]
    except KeyError:
        full_df = df

    Storage.save_data(symbol, md_type, full_df)


if __name__ == '__main__':
    asyncio.run(main())
