import random
from typing import Union, Optional

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import timedelta

from cns_analytics.entities import Duration, Symbol, Direction
from cns_analytics.timeseries import TimeSeries


class MaskAddon:
    def __init__(self, ts: TimeSeries):
        self.ts: TimeSeries = ts

    def drop_from_local_high(self, window: Duration,
                             drop_pts: float = None, drop_pct: float = None,
                             symbol: Union[Symbol, str] = None) -> np.ndarray:
        """Mask is true when price dropped more than drop_pts or drop_pct*price from local high.
        Local high is calculated as max price in window.
        drop_pts is prioritized."""
        symbol = self.ts.expect_one_symbol(symbol)
        df = self.ts.get_raw_df()[symbol]
        window = pd.Timedelta(window)
        drop = drop_pts if drop_pts else df * drop_pct
        mask = ((df.rolling(window=window).max() - df) > drop).values
        return mask

    def trending(self, trend_size: Union[Duration, int],
                 step: Duration = '1d',
                 direction: Direction = Direction.UP,
                 symbol: Union[Symbol, str] = None) -> np.ndarray:
        """Mask is true when previous `step` steps were going at specified direction (up/down).
        In case `trend_size` is Duration, than previous `trend_size` calendar days
        are taken as window, not trading days.
        """
        symbol = self.ts.expect_one_symbol(symbol)
        df = self.ts.get_raw_df()[symbol].to_frame(symbol)

        if isinstance(trend_size, (pd.Timedelta, timedelta, str)):
            trend_size = int(pd.Timedelta(trend_size) / pd.Timedelta(step))

        res_df = df.resample(pd.Timedelta(step), origin='start').first().dropna()
        sign = 1 if direction is Direction.UP else -1
        mask = ((sign * res_df.diff().dropna()) > 0).rolling(
            trend_size, min_periods=trend_size).min().astype(np.bool)
        mask = mask.reindex(df.index, method="nearest")
        df['_mask'] = mask
        df = df.fillna(method='ffill').fillna(value=False)
        # mdf = df.resample(pd.Timedelta(step), origin='start').first().dropna()
        # plt.plot(mdf[symbol].mask(mdf['_mask'].values.astype(np.bool)))
        # plt.plot(mdf[symbol].mask(~mdf['_mask'].values.astype(np.bool)))
        # plt.show()
        return df['_mask'].values

    def below_line(self,
                   line: Union[float, np.ndarray, pd.DataFrame, TimeSeries],
                   symbol: Union[Symbol, str] = None) -> np.ndarray:
        """Mask is true when price below `line`"""
        symbol = self.ts.expect_one_symbol(symbol)
        if isinstance(line, str):
            line = self.ts.get_raw_df()[line]

        df = self.ts.get_raw_df()[symbol]
        mask = df.values < line
        return mask.reshape(-1)

    def above_line(self,
                   line: Union[str, float, np.ndarray, pd.DataFrame, TimeSeries],
                   symbol: Union[Symbol, str] = None) -> np.ndarray:
        """Mask is true when price above `line`"""
        symbol = self.ts.expect_one_symbol(symbol)
        if isinstance(line, str):
            line = self.ts.get_raw_df()[line]

        df = self.ts.get_raw_df()[symbol].to_frame(symbol)
        mask = df.values > line
        return mask.reshape(-1)

    def rises_fast(self, window: Duration, growth: float, symbol: Union[Symbol, str] = None,
                   framed: bool = True) -> np.ndarray:
        symbol = self.ts.expect_one_symbol(symbol)
        df = self.ts.get_df(framed=framed)[symbol].to_frame(symbol)
        mean = df.rolling(pd.Timedelta(window)).mean()
        mask = df > (mean + growth)
        return mask.values.reshape(-1)

    def above_volatility(self, window: Duration, volatility: float, symbol: Union[Symbol, str] = None) -> np.ndarray:
        symbol = self.ts.expect_one_symbol(symbol)
        df = self.ts.get_raw_df()[symbol].to_frame(symbol)
        coef = pd.Timedelta(window) / pd.Timedelta(days=365)
        data = df.rolling(pd.Timedelta(window)).std() / coef / df * 100
        mask = data > volatility
        return mask.values.reshape(-1)
    
    
    def rising(self, periods: int = 1, threshold: float = 0, framed: bool = True) -> pd.Series:
        return self.ts.get_df(framed=framed).diff(periods) >= threshold

    def autoregression(self, *, gt: bool = False, threshold: float = 0):
        df = self.ts.get_framed_df()
        if gt:
            mask = (df > threshold).shift().fillna(False)
        else:
            mask = (df < threshold).shift().fillna(False)

        return mask

    @staticmethod
    def random_like(another_mask: np.ndarray) -> np.ndarray:
        """Returns randomly shifted `another_mask`"""
        mask = np.roll(another_mask, random.randrange(
            int(another_mask.size * 0.1), int(another_mask.size * 0.9)))
        return mask

    @staticmethod
    def random_dropout(another_mask: np.ndarray, keep=0.2) -> np.ndarray:
        """Returns copy of `another_mask` with some true values set to false
        :param another_mask: Mask to copy
        :param keep: How many true values to keep (from 0 to 1)
        """
        if 0 > keep > 1:
            raise Exception("`keep` must be in range [0, 1]")

        mask = (another_mask.astype(np.bool) * np.random.random(another_mask.shape)) > 1 - keep

        return mask

    def plot(self, mask, symbol: Optional[Union[Symbol, str]] = None, framed: bool = True):
        symbol = self.ts.expect_one_symbol(symbol)

        df = self.ts.get_df(framed=framed)[symbol]
        plt.plot(df.mask(mask), alpha=0.3, linewidth=1)
        plt.plot(df.mask(~mask), linewidth=2)
        plt.show()

    def localize(self, mask, index, symbol: Optional[Union[Symbol, str]] = None):
        symbol = self.ts.expect_one_symbol(symbol)
        mask = pd.Series(mask, index=index, name='mask')
        df = self.ts.get_raw_df()[symbol]

        df = pd.merge_asof(df, mask.to_frame('mask'), left_index=True, right_index=True)

        return df['mask'].values
