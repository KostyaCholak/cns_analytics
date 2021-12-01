from datetime import datetime
from collections import defaultdict

import matplotlib.pyplot as plt
import pytz
import pandas as pd
import numpy as np

from cns_analytics.entities import Duration
from cns_analytics.timeseries import TimeSeries


def expand_step_schema(step_schema, end_pos=None):
    _schema = {}
    _last_valid = None
    for key in step_schema.keys():
        if key < 0:
            raise Exception("Step schema must contain only positive numbers")
    if min(step_schema.keys()) != 0:
        raise Exception("Step schema must contain zero")
    for i in range(abs(max(end_pos or 0, max(step_schema.keys()))) + 1):
        _schema[i] = step_schema.get(i, _last_valid)
        _last_valid = _schema[i]
    step_schema = defaultdict(lambda: _last_valid)
    step_schema.update(_schema)
    return step_schema


def get_fix(*, data, step=None, trend=None, width=None, sl_pos=None, buy=True, reverse=False,
            book_spread=0, one_side_fee=0, early_exit=False, initial_pos=0,
            exit_on_sl=False, entry_mask=None, max_pos=None):
    sell = not buy

    if isinstance(data, TimeSeries):
        data = data.get_raw_df()[data.expect_one_symbol()]

    if trend is None:
        assert width is None
        trend = data
        width = -1e9

    if entry_mask is None:
        entry_mask = data != np.nan

    data = data.to_frame('px')
    # print('step', step)

    data['trend'] = trend
    data['mask'] = entry_mask
    next_buy = None
    next_sell = None
    reverse = -1 if reverse else 1

    open_money = 0
    position = None
    fee = 0

    reval_history = []
    closes_history = []
    position_history = []
    closes = 0
    fix_only = 0
    fix_only_history = []

    data = data.reset_index()
    last_ts = None

    last_fix = 0
    last_day_fix = 0
    fix_per_day = {}

    _bak = 1
    
    if not isinstance(step, dict):
        step = {0: step}

    step = expand_step_schema(step, end_pos=max(sl_pos or 0, max_pos or 0))
    current_step = step[0]

    levels_stack = []

    for ts, px, _trend, _entry_mask in data.values:
        buy_px = px + book_spread * reverse
        sell_px = px - book_spread * reverse

        last_ts = ts

        if not position:
            if early_exit and closes > 0:
                break
            if _entry_mask:
                if buy:
                    if position is None:
                        # open initial position, once
                        open_money -= initial_pos * buy_px
                        position = initial_pos
                        for _ in range(initial_pos):
                            levels_stack.append(buy_px)
                    next_buy = buy_px
                    next_sell = buy_px + current_step * 2
                    current_step = step[abs(position or 0)]
                else:
                    if position is None:
                        # open initial position, once
                        open_money += initial_pos * sell_px
                        position = -initial_pos
                        for _ in range(initial_pos):
                            levels_stack.append(buy_px)
                    next_buy = sell_px - current_step * 2
                    next_sell = sell_px
                    current_step = step[abs(position or 0)]

        if next_buy is not None:
            if buy_px <= next_buy and ((buy_px < _trend - width and buy) or
                                       (sell and position * reverse < 0)) and \
                    (max_pos is None or position < max_pos):
                if position != 0 or sell or _entry_mask:
                    open_money -= buy_px * reverse
                    position += 1 * reverse
                    fee += one_side_fee
                    # print(ts.date(), 'buy', round(px, 3), position)
                    old_step = current_step
                    new_step = step[abs(position or 0)]

                    if sell:
                        closes += 1
                        older_step = step.get(abs(position) - 1, step[0])
                        fix_only += levels_stack.pop() - buy_px - one_side_fee * 2
                        next_sell = next_buy + new_step
                        next_buy -= older_step
                    else:
                        levels_stack.append(buy_px)
                        next_buy -= new_step
                        next_sell = next_buy + new_step + old_step
                    current_step = new_step
                    # print('buy', position, old_step, round(px), round(next_buy), round(next_sell))

            elif sell_px >= next_sell and ((sell_px > _trend + width and sell) or
                                           (buy and position * reverse > 0)) and \
                    (max_pos is None or -position < max_pos):
                if position != 0 or buy or _entry_mask:
                    open_money += sell_px * reverse
                    position -= 1 * reverse
                    fee += one_side_fee
                    old_step = current_step
                    new_step = step[abs(position or 0)]
                    # print(ts.date(), 'sell', round(px, 3), position)
                    if buy:
                        closes += 1
                        fix_only += sell_px - levels_stack.pop() - one_side_fee * 2
                        older_step = step.get(abs(position) - 1, step[0])
                        next_buy = next_sell - new_step
                        next_sell += older_step
                    else:
                        levels_stack.append(sell_px)
                        next_sell += new_step
                        next_buy = next_sell - new_step - old_step
                    current_step = new_step
                    # print('sell', position, old_step, round(px), round(next_buy), round(next_sell))

        fix_only_history.append(fix_only)
        reval_history.append(open_money + (position or 0) * px - fee)
        closes_history.append(closes)

        if False:
            date = ts.date()
            if date not in fix_per_day:
                last_day_fix = last_fix

            fix_per_day[date] = closes - last_day_fix
            last_fix = closes

        if sl_pos is not None and abs(position or 0) >= sl_pos:
            # print('loss', 'buy' if buy else 'sell')
            if exit_on_sl:
                break
            else:
                open_money += (position or 0) * (buy_px if (position or 0) < 0 else sell_px)
                position = 0

        position_history.append(position)

    if isinstance(last_ts, datetime):
        last_ts = last_ts.astimezone(pytz.UTC)

    closes_history = np.asarray(closes_history)
    reval_history = np.asarray(reval_history)
    position_history = np.asarray(position_history)
    fix_only_history = np.asarray(np.asarray(fix_only_history))

    if isinstance(data, pd.DataFrame):
        result = pd.DataFrame({
            'reval': reval_history,
            'fix': fix_only_history,
            'pos': position_history,
        }, index=data['time'], columns=['reval', 'fix', 'pos'])

    if fix_per_day:
        breakpoint()

    return result, last_ts


def get_intersections(data, trend, width: float, reset_on_zero: bool = True):
    detrended = data - trend

    intersections_up = list(detrended[(detrended > width).astype(int).diff() == -1].index)
    intersections_down = list(detrended[(detrended < -width).astype(int).diff() == 1].index)
    intersections_zero = list(detrended[(detrended < 0).astype(int).diff() == 1].index)

    intersections = [(x, 'up') for x in intersections_up] + \
                    [(x, 'down') for x in intersections_down] + \
                    [(x, 'zero') for x in intersections_zero]

    intersections.sort(key=lambda x: x[0])

    state = 0
    count_up = 0
    count_down = 0

    for timestamp, side in intersections:
        if side == 'up' and (state == 0 or state == 1):
            state = -1
            count_up += 1
        elif side == 'down' and (state == 0 or state == -1):
            state = 1
            count_down += 1
        elif side == 'zero' and reset_on_zero:
            state = 0

    return count_up, count_down


def get_time_spaced_fix(ts: TimeSeries, time_step: Duration, loss_position: int, symbol=None,
                        buy=True):
    time_step = pd.Timedelta(time_step)

    df = ts.get_raw_df()

    if symbol is None:
        [symbol] = df.columns

    side = 1 if buy else -1

    df = df[symbol]

    last_px = df.iloc[0]

    money = -last_px * side
    position = 1 * side

    position_history = []
    reval_history = []
    px_history = []

    for point in ts.get_datetime_iterator(step=time_step):
        px = df.iloc[df.index.get_loc(point, method='nearest')]

        if abs(position) >= loss_position:
            money += position * px
            position = 0

        if (px < last_px and position < 0) or (position >= 0 and buy):
            money -= px
            position += 1
            last_px = px
        elif (px > last_px and position > 0) or (position <= 0 and not buy):
            money += px
            position -= 1
            last_px = px
        last_px = px

        position_history.append(position)
        reval_history.append(money + position * px)
        px_history.append(px)

    return np.asarray(reval_history), np.asarray(position_history), np.asarray(px_history)
