import numpy as np
import pandas as pd
import ta.momentum

from cns_analytics.entities import Duration


def sma(series: pd.Series, window: Duration):
    return series.rolling(window).mean()


def rsi(series: pd.Series, window: Duration):
    return ta.momentum.rsi(series, window)