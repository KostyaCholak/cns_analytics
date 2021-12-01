""" Defines Image class
Example usage:
with Image(save='/tmp/res/', auto_idx=True) as img:
    img.add(spread)
    img.add(spread.get_raw_df().SPREAD)
    img.add(extra['open_time'], color='black')
    img.add(time_point, color='black')
"""
import os.path
from collections import Counter
from datetime import datetime
from typing import Union, List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from cns_analytics import TimeSeries


DIR_IDX = Counter()


class Image:
    """Adds abstraction over matplotlib"""
    def __init__(self, show=None, save_to=None, title=None, fig_size=(8, 8)):
        if show is None and save_to is None:
            show = True

        self.show = show
        self.save = save_to
        self.title = str(title) if title is not None else None
        self.fig = plt.figure(figsize=fig_size)

    @classmethod
    def show(cls, data, **kwargs):
        with cls() as img:
            img.add(data, **kwargs)

    def splitv(self):
        pass

    def splith(self):
        pass

    def add(self, something: Union[TimeSeries, pd.DataFrame, pd.Series, tuple,
                                   np.ndarray, List, float, pd.Timestamp, datetime], style='line', **kwargs):
        if isinstance(something, TimeSeries):
            plt.plot(something.get_raw_df(), **kwargs)
        elif isinstance(something, float) or isinstance(something, int):
            plt.axhline(something, **kwargs)
        elif isinstance(something, pd.Timestamp) or isinstance(something, datetime):
            plt.axvline(something, **kwargs)
        elif isinstance(something, tuple):
            plt.plot(*something, 'rx')
        else:
            if style == 'line':
                plt.plot(something, **kwargs)
            elif style == 'step':
                if isinstance(something, np.ndarray):
                    x = np.arange(0, len(something), 1)
                    y = something
                else:
                    raise NotImplementedError
                plt.step(x, y, **kwargs)

    def heatmap(self, data):
        sns.heatmap(data, vmin=0.5)

    def addh(self, data, *args, **kwargs):
        plt.axhline(data, **kwargs)

    def bar(self, data, *args, **kwargs):
        plt.bar(data.index, data, width=5, **kwargs)

    def addv(self, data, *args, **kwargs):
        plt.axvline(data, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.title:
            plt.title(self.title)

        if self.save:
            is_dir = os.path.isdir(self.save)
            if is_dir:
                filepath = f"{self.save}/{DIR_IDX[self.save]}.png"
                DIR_IDX[self.save] += 1
            else:
                filepath = self.save

            plt.savefig(filepath, dpi=100)

        if self.show:
            plt.show()

        plt.close(self.fig)
        return False



