from datetime import time

import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

from cns_analytics import Duration, Image


def _timedelta_to_time(time_delta: Duration):
    if time_delta == pd.Timedelta('1d'):
        time_delta -= pd.Timedelta('1s')
    seconds = int(pd.Timedelta(time_delta).total_seconds())
    hours = seconds // 3600
    minutes = (seconds - hours * 3600) // 60
    return time(
        hour=hours,
        minute=minutes,
        second=seconds % 60
    )


class TimeDistribution:
    def __init__(self, df: pd.Series):
        self.df = df

    def get_slice(self, time_span: Duration):
        start = pd.Timedelta('0d')
        end = pd.Timedelta('1d')
        cursor = start

        bins = []
        mean = []
        size = []

        while cursor < end:
            res = self.df.between_time(
                _timedelta_to_time(cursor),
                _timedelta_to_time(cursor + time_span))
            mean.append(float(res.mean()))
            size.append(len(res))
            bins.append(cursor)
            cursor += time_span

        return bins, mean, size

    def get(self, time_span: Duration, datetime_span: Duration):
        time_span = pd.Timedelta(time_span)
        datetime_span = pd.Timedelta(datetime_span)
        dt_start = self.df.index[0]
        dt_end = self.df.index[-1]
        dt_cursor = dt_start

        dt_data = []
        dt_bins = []

        while dt_cursor < dt_end:
            start = pd.Timedelta('0d')
            end = pd.Timedelta('1d')
            cursor = start

            bins = []
            data = []
            data_len = []

            while cursor < end:
                res = self.df[dt_cursor: dt_cursor + datetime_span].between_time(
                    _timedelta_to_time(cursor),
                    _timedelta_to_time(cursor + time_span))
                data.append(res.mean())
                data_len.append(len(res))
                bins.append(cursor)
                cursor += time_span

            dt_data.append(data)
            dt_bins.append(dt_cursor)
            dt_cursor += datetime_span

        data = pd.DataFrame(np.asarray(dt_data).T).dropna().T
        breakpoint()

        sns.heatmap(data.values)
        # data.iloc[0].plot.bar()
        plt.show()

        breakpoint()
        pass

    def get_time_span(self):
        time_diff = pd.Series(self.df.index).diff()
        time_step = time_diff.mode().iloc[0]
        time_jump = time_diff[time_diff > time_step].mode().iloc[0]
        self.df[(time_diff == time_jump).values].time
        breakpoint()
        self.df.index.time