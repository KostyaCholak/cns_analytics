import numpy as np
import pandas as pd


def get_stationary_random_price(base_df: pd.DataFrame):
    """Only works for series with stationary trends"""
    df = base_df.copy()
    first_px = base_df.iloc[0]
    df['px_open'] = 1-(1-(df['px_open'].diff() / base_df['px_open']))
    df['px_close'] = 1-(1-df['px_close'].diff() / base_df['px_close'])
    df['px_high'] = 1-(1-df['px_high'].diff() / base_df['px_high'])
    df['px_low'] = 1-(1-df['px_low'].diff() / base_df['px_low'])

    df = df.sample(frac=1).set_index(base_df.index)

    breakpoint()

    df['px_open'] = df['px_open'].cumprod() + first_px['px_open']
    df['px_close'] = df['px_close'].cumprod() + first_px['px_close']
    df['px_high'] = df['px_high'].cumprod() + first_px['px_high']
    df['px_low'] = df['px_low'].cumprod() + first_px['px_low']

    return df


def get_random_walk_price(base_df: pd.DataFrame):
    df = base_df.copy()

    walk = np.cumprod(1 - ((np.random.random(len(df)) > 0.5) * 2 - 1) * 0.004)

    df['px_open'] *= walk
    df['px_close'] *= walk
    df['px_high'] *= walk
    df['px_low'] *= walk

    return df
