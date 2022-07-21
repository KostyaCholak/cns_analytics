import random
import time
from collections import defaultdict, Counter
from typing import List

import numpy as np
import pandas as pd
from scipy import stats

from cns_analytics import Symbol
from cns_analytics.backtest.techstat import get_mask_generator, prepare_data
from cns_analytics.backtest.techstat.random_mask import RandomMaskType, downsize_mask, \
    clean_mask_spaced_fast


class ThresholdTable:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def get_data(self):
        return self._df

    @classmethod
    def from_saved(cls, symbol: Symbol, hold_minutes: int):
        df = pd.read_csv(f'./.cache/thresholds/{symbol.exchange.name}_{symbol.name}_{hold_minutes}_new.csv').dropna().T
        df.set_index(df.index.astype(int), inplace=True)
        return ThresholdTable(df)

    @classmethod
    async def generate(
            cls, symbol: Symbol, hold_minutes: int,
            signals_min=600, signals_max=12000,
            calculate_rows=25, random_masks_count=50000
    ):
        minutes = 5

        dfs, frs = await prepare_data([symbol.name], minutes, hold_minutes)
        df, fr = dfs[0], frs[0]
        df_size = len(df)

        mask_generator = get_mask_generator(RandomMaskType.SIMPLE)

        save_res = defaultdict(list)
        size_range = np.geomspace(signals_min, signals_max, calculate_rows).round().astype(int)

        for signals_num in size_range:
            print(f'Loading {symbol.name}, mins={hold_minutes}, signs={signals_num}                 ', end='\r')
            result = []
            for _ in range(random_masks_count):
                rnd_mask = mask_generator(signals_num, df_size)

                result.append(round(fr[rnd_mask].mean(), 3))

            save_res[signals_num] = sorted(result)

        res = pd.DataFrame(save_res)
        res.to_csv(f'./.cache/thresholds/{symbol.exchange.name}_{symbol.name}_{hold_minutes}.csv')

        return cls.from_saved(symbol, hold_minutes)

    @classmethod
    async def generate_new(
            cls, symbol: Symbol, hold_minutes: int,
            signals_min=600, signals_max=12000,
            calculate_rows=25, random_masks_count=50000
    ):
        minutes = 5

        dfs, frs = await prepare_data([symbol.name], minutes, hold_minutes)
        df, fr = dfs[0], frs[0]
        df_size = len(df)

        mask_generator = get_mask_generator(RandomMaskType.SIMPLE)

        save_res = defaultdict(list)
        signals_removed_count = Counter()
        signals_removed = Counter()
        size_range = np.geomspace(signals_min, signals_max, calculate_rows).round().astype(int)
        for size in size_range:
            # for ordering
            save_res[size] = []
            signals_removed[size] = 0
            signals_removed_count[size] = 0

        try:
            df = pd.read_csv(f'./.cache/thresholds/{symbol.exchange.name}_{symbol.name}_{hold_minutes}_new.csv')
            for size in size_range:
                column = str(int(size))
                if column in df.columns:
                    values = df[column].dropna().tolist()
                    save_res[size].extend(values)
        except FileNotFoundError:
            pass

        for _ in range(random_masks_count):
            sss = [len(saved) for saved in save_res.values()]
            if _ > 3000:
                signals_num = size_range[np.argmin(sss)]
            else:
                signals_num = random.choice(size_range)

            if signals_num >= size_range[-1]:
                next_signals_num = signals_num * size_range[0]
            else:
                next_signals_num = size_range[np.argmax(signals_num < size_range)]
            new_signals_num = int(signals_num + (signals_removed.get(signals_num) / (signals_removed_count.get(signals_num) or 1)))
            rnd_mask = mask_generator(new_signals_num, df_size)
            rnd_mask = clean_mask_spaced_fast(rnd_mask, fr.index.astype(int).values / 1e9, hold_minutes)
            rnd_mask = pd.Series(rnd_mask, index=fr.index)
            rnd_mask = downsize_mask(rnd_mask, size_range)
            if rnd_mask is None:
                continue
            upd_signals_num = int(np.sum(rnd_mask))
            signals_removed_count[signals_num] += 1
            upd = abs(new_signals_num - upd_signals_num) * 3
            signals_removed[signals_num] += upd if upd_signals_num < next_signals_num else -upd
            signals_removed[signals_num] = max(0, signals_removed[signals_num])

            if _ >= 100 and _ % 100 == 0:
                abc = [int(round(signals_removed[x] / (signals_removed_count[x] or 1))) for x in signals_removed_count]
                print(max(sss) - min(sss))
                print('a', abc)
                print('b', sss)
            save_res[upd_signals_num].append(round(fr[rnd_mask].mean(), 3))

        max_saved = max([len(saved) for key, saved in save_res.items()])
        for key in save_res:
            save_res[key] = sorted(save_res[key])
            save_res[key].extend([None] * (max_saved - len(save_res[key])))

        res = pd.DataFrame(save_res)
        res.to_csv(f'./.cache/thresholds/{symbol.exchange.name}_{symbol.name}_{hold_minutes}_new.csv', index=False)

        return cls.from_saved(symbol, hold_minutes)

    def get_confidence(self, signals_count: float, finrez: float):
        if signals_count < int(self._df.index[0]):
            return None
        if signals_count > int(self._df.index[-1]):
            return None
        threshold_line = self._df.iloc[self._df.index.get_loc(signals_count, method='nearest')]
        return stats.percentileofscore(threshold_line, finrez)

    @classmethod
    def combine(cls, symbols: List[Symbol], hold_minutes, weights):
        tables = [ThresholdTable.from_saved(symbol, hold_minutes) for symbol in symbols]
        thresholds = [table.get_data() * weight for table, weight in zip(tables, weights)]
        df = pd.DataFrame(np.sum(thresholds, axis=0) / len(thresholds),
                          columns=thresholds[0].columns,
                          index=thresholds[0].index)
        df = df[~df.index.duplicated(keep='first')] / np.sum(weights)

        return ThresholdTable(df)

