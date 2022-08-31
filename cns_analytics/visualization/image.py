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

import numpy as np
import pandas as pd

from cns_analytics import TimeSeries


DIR_IDX = Counter()


class Image:
    """Adds abstraction over matplotlib"""
    def __init__(self, show=None, save_to=None, title=None, fig_size=(8, 8), draw=False):
        import matplotlib.pyplot as plt

        if show is None and save_to is None:
            show = True

        self._show = show
        self._draw = draw
        self._save = save_to
        self._title = str(title) if title is not None else None
        self._fig = plt.figure(figsize=fig_size)

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
        import matplotlib.pyplot as plt

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
        import seaborn as sns

        sns.heatmap(data, vmin=0.5)

    def addh(self, data, **kwargs):
        import matplotlib.pyplot as plt

        plt.axhline(data, **kwargs)

    def addv(self, data, **kwargs):
        import matplotlib.pyplot as plt

        plt.axvline(data, **kwargs)

    def bar(self, data, width=5, **kwargs):
        import matplotlib.pyplot as plt

        plt.bar(data.index, data, width=width, **kwargs)

    def hist(self, data, bins=20, **kwargs):
        import matplotlib.pyplot as plt

        plt.hist(data, bins=20, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        import matplotlib.pyplot as plt

        if exc_type is not None:
            return False

        if self._title:
            plt.title(self._title)

        if self._save:
            is_dir = os.path.isdir(self._save)
            if is_dir:
                filepath = f"{self._save}/{DIR_IDX[self._save]}.png"
                DIR_IDX[self._save] += 1
            else:
                filepath = self._save

            plt.savefig(filepath, dpi=100)

        if self._show:
            if self._draw:
                plt.draw()
                plt.pause(0.0001)
            else:
                plt.show()

        plt.close(self._fig)
        return False



