import asyncio
import random

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.widgets import MultiCursor

from ta.momentum import rsi, roc
from ta.volatility import ulcer_index

# from cns_analytics import TimeSeries, Exchange, Symbol, Image
from cns_analytics.backtest.techstat.random_mask import clean_mask_spaced_fast, after_ok
# from random_mask import clean_mask_spaced_fast, after_ok

plt.rcParams['figure.constrained_layout.use'] = True


def clean(mask, keep_for):
    clean_mask_spaced_fast(mask.values, mask.index.astype(int).values / 1e9, keep_for)
    return mask


def _load_data(filename):
    df = pd.read_csv(filename, index_col='ts', parse_dates=True)
    return df


class Cursor:
    def __init__(self, ax):
        self.ax = ax
        self.vertical_line = ax.axvline(color='k', lw=0.8, ls='--')

    def set_cross_hair_visible(self, visible):
        need_redraw = self.vertical_line.get_visible() != visible
        self.vertical_line.set_visible(visible)
        return need_redraw

    def on_mouse_move(self, event):
        if not event.inaxes:
            need_redraw = self.set_cross_hair_visible(False)
            if need_redraw:
                self.ax.figure.canvas.draw()
        else:
            self.set_cross_hair_visible(True)
            x, y = event.xdata, event.ydata
            self.vertical_line.set_xdata(x)
            self.ax.figure.canvas.draw()


class Analytics:
    def __init__(self):
        self._data = None
        self._full_data = None
        self.resolution = '1T'

    async def main(self):
        # ts = TimeSeries(Symbol('AAPL', Exchange.Barchart))
        # await ts.load_ohlc()
        # ts.resample(self.resolution)
        # data = ts.get_raw_df()
        data = _load_data('/tmp/aapl_1m.csv')
        self._data = data.iloc[:50000]
        self._full_data = data
        # self.update()

        indicators, rule_buy = open('config.txt').read().strip().split('---')

        lines = [x for x in indicators.strip().split('\n') if x]
        rule_buy = rule_buy.strip()

        lines.insert(0, 'df.px_close')

        fig, axes = plt.subplots(len(lines), gridspec_kw={
            'height_ratios': [5, ] + [1 for _ in lines][1:]
        }, sharex=True)

        keep_for = 10

        partial_fr = -self._data.px_close.diff(-keep_for)

        for ax, line in zip(axes, lines):
            result = eval(line, globals(), {
                'df': self._data,
                'fr': partial_fr
            })
            ax.tick_params(axis="y", direction="in", pad=-26)
            ax.tick_params(axis="x", direction="in", pad=-15)
            ax.plot(result.values, lw=1)
            ymin = np.nanpercentile(result, .25)
            ymax = np.nanpercentile(result, 99.75)
            ax.set_ylim([ymin, ymax])
            ax.grid(color='#9e9e9e', linestyle='--', linewidth=0.3)

        mask = eval(rule_buy, globals(), {
            'df': self._data,
            'fr': partial_fr
        })

        clean_mask_spaced_fast(mask.values, self._data.index.astype(int).values / 1e9, keep_for)

        result = mask.to_frame('signal')
        result['px_close'] = self._data.px_close

        full_fr = self._full_data.px_close
        fr = -full_fr.diff(-keep_for)
        full_mask = eval(rule_buy, globals(), {
            'df': self._full_data,
            'fr': fr
        }).astype(bool)

        fr2 = fr[full_mask]
        fr_gt_0 = (fr2 >= 0).sum()
        fr_lt_0 = (fr2 < 0).sum()

        # with Image() as img:
        #     img.add((fr >= 0).cumsum())
        #     img.add((fr < 0).cumsum())
        #
        # with Image() as img:
        #     img.add(fr2.cumsum())

        for i, row in enumerate(result.itertuples()):
            if row.signal:
                finish = result.iloc[i + keep_for]
                _fr = float(finish.px_close) - float(row.px_close)
                color = 'red' if _fr < 0 else 'green'
                axes[0].plot([i, i + keep_for], [float(row.px_close), float(finish.px_close)],
                             color=color, linestyle='-', marker='x',
                             markeredgecolor='blue', markersize=9, lw=2,
                             markevery=[0])

        print(fr2.sum(), fr2.mean())
        print(fr_gt_0, fr_lt_0, fr_gt_0 / fr_lt_0)

        multi = MultiCursor(fig.canvas, axes, color='r', lw=1)
        plt.draw()
        plt.show()


an = Analytics()

asyncio.run(an.main())
