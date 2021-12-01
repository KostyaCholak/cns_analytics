"""Simplifies work with ohlc bars

| This module realizes OHLC class which simplifies work with OHLC bars.
| OHLC stands for Open, High, Low, Close prices inside fixed time period.
| For example:
| - open  1.3
| - high  1.4
| - low   1.1
| - close 1.2
| for 5-minute bar means that
  inside those five minutes the first market deal was at price 1.1, last at 1.2,
  highest price was 1.4 and lowest was 1.1.
| Bars can follow one another in time (bar starts where previous ends)
  or bars can go with some fixed interval one over another
  (for example bars are one hour big, but there is a bar every 1 minute).
| By default first behaviour is used, to change it use "rolling_backwards" when initializing OHLC
"""
from typing import Optional

import pandas as pd

from cns_analytics.entities import Duration


class OHLCMask:
    def __init__(self, ohlc: 'OHLC'):
        self.ohlc = ohlc

    def rising(self, pct: float = 1, num: int = 1) -> pd.Series:
        """Selects bars that have `pct`% rising bars of the `num` previous bar"""
        return (self.ohlc.df['body'] > 0).rolling(num).mean() >= pct

    def falling(self, pct: float = 1, num: int = 1) -> pd.Series:
        """Selects bars that have `pct`% falling bars of the `num` previous bar"""
        return (self.ohlc.df['body'] < 0).rolling(num).mean() >= pct


class OHLC:
    """Represents a collection of OHLC bars"""
    def __init__(self, df: pd.DataFrame, resolution: Optional[Duration] = None, rolling_backwards=False,
                 open_field='px_open', high_field='px_high',
                 low_field='px_low', close_field='px_close', volume_field='volume'):
        """Initialises OHLC instance from pandas series

        :param df: Series to initialize from
        :param resolution: Resolution to resample bars
        :param rolling_backwards: Create bar for every point in df, not every 1 resolution
        """

        self.mask = OHLCMask(self)

        if resolution is not None:
            if isinstance(resolution, str):
                resolution = pd.Timedelta(resolution)

            idx = pd.date_range(df.index[0], df.index[-1], freq='1T')
            df = df.reindex(idx, method='ffill')

            if rolling_backwards:
                self.df = df.shift(periods=1, freq=resolution).to_frame('open')
                self.df['high'] = df.rolling(resolution).max()
                self.df['low'] = df.rolling(resolution).min()
                self.df['close'] = df
            else:
                self.df = df.resample(resolution).ohlc()
        else:
            self.df = df.copy()
            self.df.rename(columns={
                open_field: "open",
                high_field: "high",
                low_field: "low",
                close_field: "close",
                volume_field: "volume",
            }, inplace=True)

        self.df['body'] = self.df['close'] - self.df['open']

        self.df.dropna(inplace=True)

        self._mask = None

    def set_mask(self, new_mask):
        self._mask = new_mask

    def get_mask(self):
        return self._mask

    @property
    def masked_df(self):
        if self._mask is None:
            return self.df
        return self.df[self._mask]
