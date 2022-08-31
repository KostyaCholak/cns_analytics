import pandas as pd

from cns_analytics import TimeSeries, Symbol, Exchange
from cns_analytics.backtest.techstat.random_mask import (
    get_mask_generator,
    clean_mask_spaced,
    generate_simple_random_mask,
    generate_spaced_random_mask,
)


def clip_outliers(data: pd.Series, quantile=0.01) -> pd.Series:
    return data.clip(
        lower=data.quantile(q=quantile),
        upper=data.quantile(q=1-quantile),
    )


def forward_relative_finrez(data: pd.Series, forward_candles: int) -> pd.Series:
    fr = -data.diff(forward_candles) / data
    fr = fr / fr.abs().mean() * 100
    return fr


async def load_df(symbol, minutes):
    ts = TimeSeries(Symbol(symbol, Exchange.Barchart))
    await ts.load_ohlc()
    # ts._df = ts._df.between_time('06:45', '23:00')
    ts.resample(f'{minutes}m')
    df = ts.get_raw_df()

    return df


async def prepare_data(symbols, minutes, hold_for):
    dfs = [await load_df(symbol, minutes) for symbol in symbols]
    frs = []

    for df in dfs:
        clipped_close = clip_outliers(df.px_close, quantile=0.01)
        fr = forward_relative_finrez(clipped_close, -hold_for // minutes)
        frs.append(fr)

    return dfs, frs
