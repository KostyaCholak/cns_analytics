import enum
from datetime import time
from typing import Optional, Callable, Dict

import numba
import numpy as np
import pandas as pd
from pandas.core.groupby import SeriesGroupBy


class RandomMaskType(enum.Enum):
    SIMPLE = enum.auto()
    SPACED = enum.auto()


def generate_simple_random_mask(signals_count, mask_size) -> np.array:
    return np.random.random(mask_size) < (signals_count / mask_size)


def generate_spaced_random_mask(signals_number, full_size, min_spacing) -> np.array:
    spacing = full_size / signals_number
    variation = (spacing - min_spacing) / spacing
    try:
        assert 0 <= variation <= 1
    except AssertionError:
        breakpoint()
    mask = np.zeros(full_size)
    sigmals_min = spacing * (1 - variation)
    sigmals_max = spacing * (1 + variation)
    steps = np.random.randint(sigmals_min, sigmals_max, signals_number).round().astype(int)
    steps = np.cumsum(steps)

    steps = (steps * (full_size / (steps[-1] + 1))).round().astype(int)

    mask[steps] = 1
    return mask.astype(bool)


def get_mask_generator(mask_type: RandomMaskType) -> Callable:
    return {
        RandomMaskType.SIMPLE: generate_simple_random_mask,
        RandomMaskType.SPACED: generate_spaced_random_mask,
    }[mask_type]


def clean_mask_spaced(mask: pd.Series, hold_minutes: int):
    mask = mask.copy()
    dates = pd.Series(mask[mask].index)
    last_date = None
    hold_minutes = pd.Timedelta(f"{hold_minutes}T")

    mask[mask] = False

    for date in dates:
        if not last_date or date - last_date > hold_minutes:
            last_date = date
            mask[date] = True

    return mask


@numba.njit()
def clean_mask_spaced_fast(mask: np.array, index: np.array, hold_minutes: int):
    dates = index[mask]
    idxs = np.arange(len(mask))[mask]
    last_date = dates[0] - hold_minutes
    hold_minutes *= 60

    mask[mask] = False

    for date, idx in zip(dates, idxs):
        if date - last_date >= hold_minutes:
            last_date = date
            mask[idx] = True


def clean_mask_from_overnight(mask: pd.Series, trading_end_time: str, hold_minutes: int) -> pd.Series:
    mask = mask.copy()
    hours, minutes = [int(x) for x in trading_end_time.split(':')]
    hours -= hold_minutes // 60
    minutes += hold_minutes % 60
    limit = time(hours, minutes % 60)
    mask[mask.index.time > limit] = False
    return mask


def downsize_mask(mask: pd.Series, size_options: np.array) -> Optional[pd.Series]:
    mask = mask.copy()
    mask_reset = mask.reset_index(drop=True)
    dates = list(mask_reset[mask_reset].index)
    mask_size = np.sum(mask)
    if mask_size < np.min(size_options):
        return None
    if mask_size > np.max(size_options):
        new_mask_size = size_options[-1]  # np.random.choice(mask_sizes)
    else:
        new_mask_size = size_options[np.diff((mask_size < size_options)).argmax()]
    kkk = np.random.choice(dates, mask_size - new_mask_size, replace=False)
    mask.iloc[kkk] = False

    return mask


def normalize_freq(group_by: SeriesGroupBy) -> pd.Series:
    intermediate = group_by.sum() / group_by.size()

    return intermediate / intermediate.sum()


def get_mask_freqs(mask: pd.Series):
    index = mask.index.isocalendar()
    years = normalize_freq(mask.groupby(index.year))
    months = normalize_freq(mask.groupby(mask.index.month))
    weeks = normalize_freq(mask.groupby(index.week % 4))
    weekdays = normalize_freq(mask.groupby(index.day))
    hours = normalize_freq(mask.groupby(mask.index.hour))
    minutes = normalize_freq(mask.groupby(mask.index.minute))

    return years, months, weeks, weekdays, hours, minutes


def get_mask_stats(mask: pd.Series):
    index = mask.index.isocalendar()
    years, months, weeks, weekdays, hours, minutes = get_mask_freqs(mask)

    years_pct = index.year.map(years)
    months_pct = mask.index.month.map(months).to_series(index=mask.index)
    weeks_pct = (index.week % 4).map(weeks)
    weekdays_pct = index.day.map(weekdays)
    hours_pct = mask.index.hour.map(hours).to_series(index=mask.index)
    minutes_pct = mask.index.minute.map(minutes).to_series(index=mask.index)

    return years_pct, months_pct, weeks_pct, weekdays_pct, hours_pct, minutes_pct


def create_mask_from_stats(mask: pd.Series, mask_stats, mask_freqs, first_hour_mask, hold_for, max_size_deviation_pct=0.03):
    years_pct, months_pct, weeks_pct, weekdays_pct, hours_pct, minutes_pct = mask_stats
    signals_count = np.sum(mask)
    max_deviation = max(signals_count * max_size_deviation_pct, 20)
    signals_count_adjusted = signals_count / 0.35
    mask_len = len(mask)
    msk = None

    first_hour_adjustment = 1

    abc = 0
    max_error = 0.035
    while True:
        hours_pct[first_hour_mask] *= first_hour_adjustment
        pct = years_pct * months_pct * weeks_pct * weekdays_pct * hours_pct * minutes_pct * len(mask_stats)

        output_signals_count = None
        adjustments = []
        tries = []
        while output_signals_count is None or abs(output_signals_count - signals_count) > max_deviation:
            msk = (pct / pct.sum() * signals_count_adjusted).gt(np.random.random(mask_len))
            clean_mask_spaced_fast(msk.values, msk.index.astype(int).values / 1e9, hold_for)
            output_signals_count = msk.sum()
            tries.append(output_signals_count)
            adjustments.append(output_signals_count / signals_count_adjusted)
            signals_count_adjusted = signals_count / np.mean(adjustments)

        # if len(tries) > 3:
        #     print(len(tries))
        #     breakpoint()

        resulting_freqs = get_mask_freqs(msk)
        errors = []
        for st1, st2 in zip(resulting_freqs, mask_freqs):
            errors.append(round((st1 - st2).max(), 3))
        errors.append(resulting_freqs[-2].iloc[0] - mask_freqs[-2].iloc[0])
        if max([abs(x) for x in errors]) < max_error:
            break
        if errors[-1] > max_error:
            first_hour_adjustment = 0.95
        elif errors[-1] < -max_error:
            first_hour_adjustment = 1.05
        else:
            first_hour_adjustment = 1
        # print(abc, first_hour_adjustment, errors, hours_pct[first_hour_mask].mean())
        abc += 1
    return msk


# @numba.njit()
def after_ok(fr, mask, keep_for):
    mask = mask.copy()
    clean_mask_spaced_fast(mask.values, mask.index.astype(int).values / 1e9, keep_for)
    finrezes = fr[mask]
    idxs = np.arange(len(mask))[mask]
    last_finrez = None

    mask[mask] = False

    for finrez, idx in zip(finrezes, idxs):
        if last_finrez is not None and last_finrez > 0:
            mask[idx] = True
        last_finrez = finrez

    return mask
