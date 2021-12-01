import asyncio
from dataclasses import dataclass
from typing import Optional, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from cns_analytics.database import DataBase
from cns_analytics.entities import Symbol, Exchange, Duration, DateTime
from cns_analytics.timeseries import TimeSeries


@dataclass
class FlexBacktestResult:
    spread: list
    reval: list
    pos: list
    coef: list


class BacktestAddon:
    def __init__(self, ts: TimeSeries):
        self.ts = ts

    def stat_arb(self, *, tp: float, sl: float,
                 entry_mask: Optional[np.ndarray] = None,
                 symbol: Optional[Union[Symbol, str]] = None):
        symbol = self.ts.expect_one_symbol(symbol)
        df = self.ts.get_framed_df()[symbol]
        df = df.reset_index()
        df['time'] = df['time'].values.astype(np.int64) // 10 ** 9
        df['mask'] = df[symbol] if entry_mask is None else entry_mask

        timestamp_history = []
        reval_history = []
        tp_count_history = []
        sl_count_history = []

        exit_tp = None
        exit_sl = None

        tp_count = 0
        sl_count = 0

        open_money = 0
        position = 0

        for timestamp, px, mask_row in df.values:
            if exit_tp is None:
                entry_ok = entry_mask is None or mask_row
                if entry_ok:
                    exit_tp = px + tp
                    exit_sl = px - sl
                    open_money -= px
                    position = 1
            else:
                closed = False

                if px > exit_tp:
                    tp_count += 1
                    closed = True
                elif px < exit_sl:
                    sl_count += 1
                    closed = True

                if closed:
                    exit_tp = None
                    exit_sl = None
                    open_money += px
                    position = 0

            timestamp_history.append(timestamp * 1e9)
            reval_history.append(open_money + position * px)
            tp_count_history.append(tp_count)
            sl_count_history.append(sl_count)

        result_df = pd.DataFrame(reval_history, index=pd.to_datetime(timestamp_history), columns=['revaluation'])
        result_df['tp_count'] = tp_count_history
        result_df['sl_count'] = sl_count_history

        return result_df

    def strange(self, *, timestamp: DateTime, initial_pos: int, step: float, close_diapason: float,
                one_way_fee: float = 0, book_spread: float = 0,
                symbol: Optional[Union[Symbol, str]] = None):
        df = self.ts.get_df(framed=True)[symbol][timestamp:]
        spread_hist = []
        finrez_hist = []
        pos_hist = []

        opn_px = df.iloc[0]
        opn_money = initial_pos * opn_px
        position = initial_pos

        if book_spread != 0:
            raise NotImplementedError()

        for idx, spread in df.itertuples():
            position_new = round((opn_px - spread) / step)
            position_new = min(position_new, initial_pos)

            position_diff = position - position_new
            position = position_new

            opn_money += spread * position_diff
            opn_money -= abs(position_diff) * one_way_fee

            spread_hist.append(spread)
            finrez_hist.append(opn_money + spread * position)
            pos_hist.append(position)

            if opn_px - spread > close_diapason:
                break

        result = pd.DataFrame({
            'reval': finrez_hist,
            'spread': spread_hist,
            'pos': pos_hist,
        }, index=df.index, columns=['reval', 'spread', 'pos'])

        return FlexBacktestResult(
            spread=result.spread,
            reval=result.reval,
            pos=result.pos,
            coef=idx,
        )

    def flex_fix(self, *, s1: str, s2: str, step: float, flex=0.01, max_pos=None,
                 sl_pos=None, one_way_fee: float = 0, book_spread: float = 0,
                 flex_interval: Duration = '1d'):
        flex_interval = pd.Timedelta(flex_interval)
        df = self.ts.get_df(framed=True)[[s1, s2]]
        shift = (df[s1] - df[s2]).iloc[0]
        coef = 1

        spread_hist = []
        finrez_hist = []
        coef_hist = []
        pos_hist = []

        next_change = df.index[0]
        opn_money = 0
        position = 0

        if book_spread != 0:
            raise NotImplementedError()

        if sl_pos is not None:
            raise NotImplementedError()

        for idx, px1, px2 in df.itertuples():
            spread = px1 - px2 * coef - shift
            if idx > next_change:
                next_change = idx + flex_interval
                if spread > 0:
                    coef *= 1 + flex
                else:
                    coef /= 1 + flex

            position_new = -round(spread / step)
            if max_pos and abs(position_new) > max_pos:
                position_new = np.sign(position_new) * max_pos
            # if sl_pos and abs(position_new) > sl_pos:
            #     pass
            position_diff = position - position_new
            position = position_new

            opn_money += px1 * position_diff
            opn_money -= px2 * position_diff
            opn_money -= abs(position_diff) * one_way_fee

            spread_hist.append(spread)
            finrez_hist.append(opn_money + px1 * position - px2 * position)
            pos_hist.append(position)
            coef_hist.append(coef)

        result = pd.DataFrame({
            'reval': finrez_hist,
            'spread': spread_hist,
            'pos': pos_hist,
            'coef': coef_hist,
        }, index=df.index, columns=['reval', 'spread', 'pos', 'coef'])

        return FlexBacktestResult(
            spread=result.spread,
            reval=result.reval,
            pos=result.pos,
            coef=result.coef,
        )


async def main():
    DataBase.set_default_exchange(Exchange.Barchart)
    ts = TimeSeries('T10', 'T30')
    await ts.load(end='01-01-2018')
    ts['SPREAD'] = ts['T10'] * 5 - ts['T30'] * 2

    ts.exclude_symbol('T10')
    ts.exclude_symbol('T30')

    # ts.plot()

    sma1 = ts['SPREAD'].get_raw_df()
    sma2 = ts.sma('90d').get_raw_df()

    plt.plot(sma1)
    plt.plot(sma2)
    plt.show()

    # mask = ts.mask.trending(trend_size='5d', step='1d', direction=Direction.UP)
    mask = sma1 < (sma2 - 1000)
    result = ts.backtest.stat_arb(tp=500, sl=100, entry_mask=mask)
    # plt.plot(result.tp_count)
    # plt.plot(result.sl_count)
    # plt.show()
    plt.plot(result.revaluation)
    plt.show()
    breakpoint()
    pass


if __name__ == '__main__':
    asyncio.run(main())
