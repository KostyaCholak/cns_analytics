import functools
import random
from collections import defaultdict
from typing import List

import numpy as np
import ta.momentum
import ta.trend
import ta.volatility

from cns_analytics.backtest.techstat.rules import Ret

RULES_REGISTRY = {}
RULES_BY_RETURN_TYPE = defaultdict(list)


def register_rule(params: dict):
    def decorator(rule):
        RULES_REGISTRY[rule.__name__] = params
        returns = params.get('returns', Ret.BOOL_SERIES)
        RULES_BY_RETURN_TYPE[returns].append(rule.__name__)

        @functools.wraps(rule)
        def wrapper(*args, **kwargs):
            return rule(*args, **kwargs)

        return wrapper

    return decorator


@register_rule({
    'params': [[Ret.CANDLES_NUM, Ret.CANDLES_NUM]]
})
def rule_sma1_gt_sma2(df, window1, window2):
    sma1 = ta.trend.sma_indicator(df.px_close, window=window1)
    sma2 = ta.trend.sma_indicator(df.px_close, window=window2)
    return sma1 > sma2


@register_rule({
    'params': [[Ret.CANDLES_NUM, Ret.ZH_NUM]]
})
def rule_rsi_gt_num(df, window, rsi_value, column='px_close'):
    rsi_value = max(100 - rsi_value, rsi_value)
    return ta.momentum.rsi(df[column], window=window) > rsi_value


@register_rule({
    'params': [[Ret.CANDLES_NUM, Ret.ZH_NUM]]
})
def rule_rsi_lt_num(df, window, rsi_value, column='px_close'):
    rsi_value = min(100 - rsi_value, rsi_value)
    return ta.momentum.rsi(df[column], window=window) < rsi_value


# @register_rule({
#     'params': [[Ret.CANDLES_NUM, Ret.CANDLES_NUM]]
# })
# def rule_bollinger_hband(df, window, window_dev):
#     return ta.volatility.bollinger_hband_indicator(df.px_close, window, window_dev) == 1
#
#
# @register_rule({
#     'params': [[Ret.CANDLES_NUM, Ret.CANDLES_NUM]]
# })
# def rule_bollinger_lband(df, window, window_dev):
#     return ta.volatility.bollinger_lband_indicator(df.px_close, window, window_dev) == 1


@register_rule({
    'params': [[Ret.CANDLES_NUM, Ret.CANDLES_NUM, Ret.CANDLES_NUM]]
})
def rule_macd_diff_cross_up(df, window_slow, window_fast, window_signal):
    return np.sign(ta.trend.macd_diff(df.px_close, window_slow, window_fast, window_signal)).diff() > 0


@register_rule({
    'params': [[Ret.CANDLES_NUM, Ret.CANDLES_NUM, Ret.CANDLES_NUM]]
})
def rule_macd_diff_cross_down(df, window_slow, window_fast, window_signal):
    return np.sign(ta.trend.macd_diff(df.px_close, window_slow, window_fast, window_signal)).diff() < 0


@register_rule({
    'params': [[Ret.CANDLES_NUM, Ret.CANDLES_NUM, Ret.CANDLES_NUM, Ret.ZH_NUM]]
})
def rule_macd_diff_above(df, window_slow, window_fast, window_signal, value):
    return ta.trend.macd_diff(df.px_close, window_slow, window_fast, window_signal) / (df.px_close ** 2) * 50000000 > value


@register_rule({
    'params': [[Ret.CANDLES_NUM, Ret.CANDLES_NUM, Ret.CANDLES_NUM, Ret.ZH_NUM]]
})
def rule_macd_diff_below(df, window_slow, window_fast, window_signal, value):
    return ta.trend.macd_diff(df.px_close, window_slow, window_fast, window_signal) / (df.px_close ** 2) * 50000000 < -value


@register_rule({
    'params': [[]]
})
def rule_big_volume(df):
    window1 = 3 * 60
    volume = df['volume'].rolling(f'{window1}T').sum()
    return volume > volume.rolling('14d').mean()


# @register_rule({
#     'params': [[]]
# })
def rule_low_volatility(df):
    window1 = 3 * 60
    volume = df['px_close'].rolling(f'{window1}T').std()
    return volume < volume.rolling('30d').mean()


def _get_next_function(return_type: Ret) -> str:
    candidates = RULES_BY_RETURN_TYPE[return_type]
    return random.choice(candidates)


class RuleGenerator:
    def __init__(self, generic: bool = True, candles_range=(40, 1200)):
        self.generic = generic
        self.candles_range = candles_range

    def render_with_values(self, rule):
        while '<CN>' in rule:
            rule = rule.replace('<CN>', str(self.generate_candles()), 1)

        while '<MHHN>' in rule:
            rule = rule.replace('<MHHN>', str(round(random.random() * 200 - 100, 2)), 1)

        while '<ZHN>' in rule:
            rule = rule.replace('<ZHN>', str(round(random.random() * 100, 2)), 1)

        return rule

    def _render_function(self, next_func):
        next_params = self._get_params_for_function(next_func)
        if next_func == 'rule_and':
            return f"({next_params[0]} & {next_params[1]})"
        elif next_func == 'rule_below':
            return f"({next_params[0]} < {next_params[1]})"
        elif next_func == 'rule_above':
            return f"({next_params[0]} > {next_params[1]})"

        return f"{next_func.replace('rule_', 'rule_')}(df, {', '.join(next_params)})"

    def _get_params_for_function(self, func_name: str) -> List[str]:
        param_options = RULES_REGISTRY[func_name]['params']
        params = random.choice(param_options)
        res = []

        for param_type in params:
            try:
                next_func = _get_next_function(param_type)
            except IndexError:
                if param_type == Ret.CLOSE_PX:
                    res.append('df.close')
                elif param_type == Ret.VOLUME:
                    res.append('df.volume')
                elif param_type == Ret.CANDLES_NUM:
                    res.append('<CN>')
                elif param_type == Ret.ZH_NUM:
                    res.append('<ZHN>')
                elif param_type == Ret.MHH_NUM:
                    res.append('<MHHN>')
                else:
                    raise NotImplementedError(param_type)
                continue
            res.append(self._render_function(next_func))

        return res

    def generate_candles(self):
        return random.randrange(*self.candles_range)

    def generate(self, seed) -> str:
        # random.seed(seed)
        func = _get_next_function(Ret.BOOL_SERIES)
        return self._render_function(func)

    def generate_compound(self, seeds) -> str:
        rules = []
        for seed in seeds:
            rule = self.generate(seed)
            if rule in rules:
                raise ZeroDivisionError()
            rules.append(rule)

        return ' & '.join(sorted(rules))

    def generate_from_func(self, func_name: str, seed) -> str:
        # random.seed(seed)
        return self._render_function(func_name)
