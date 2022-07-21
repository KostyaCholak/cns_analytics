import functools
import random

import enum
from collections import defaultdict
from typing import List

import numpy as np
import ta.trend
import ta.momentum
import ta.volatility


class Ret(enum.Enum):
    STD_NUM = enum.auto()
    VOLUME = enum.auto()
    CLOSE_PX = enum.auto()
    BOOL_SERIES = enum.auto()
    SMA_SERIES = enum.auto()
    STD_SERIES = enum.auto()
    CANDLES_NUM = enum.auto()
    ZH_SERIES = enum.auto()
    ZH_NUM = enum.auto()
    MHH_SERIES = enum.auto()
    MHH_NUM = enum.auto()


RULES_REGISTRY = {}
RULES_BY_RETURN_TYPE = defaultdict(list)


def register_rule(params: dict):
    def decorator(rule):
        RULES_REGISTRY[rule.__name__] = params
        RULES_BY_RETURN_TYPE[params['returns']].append(rule.__name__)

        @functools.wraps(rule)
        def wrapper(*args, **kwargs):
            return rule(*args, **kwargs)

        return wrapper

    return decorator


@register_rule({
    'params': [[Ret.CLOSE_PX, Ret.CANDLES_NUM]],
    'returns': Ret.SMA_SERIES
})
def rule_sma(close, window):
    return ta.trend.sma_indicator(close, window=window)


@register_rule({
    'params': [[Ret.CLOSE_PX, Ret.CANDLES_NUM, Ret.STD_NUM]],
    'returns': Ret.STD_SERIES
})
def rule_above_pboll(close, window, std):
    return ta.volatility.bollinger_pband(close, window=window) > std


@register_rule({
    'params': [
        [Ret.CLOSE_PX, Ret.CANDLES_NUM, Ret.STD_NUM]
    ],
    'returns': Ret.STD_SERIES
})
def rule_below_pboll(close, window, std):
    return ta.volatility.bollinger_pband(close, window=window) < std


@register_rule({
    'params': [
        [Ret.CLOSE_PX, Ret.CANDLES_NUM],
        [Ret.SMA_SERIES, Ret.CANDLES_NUM]
    ],
    'returns': Ret.ZH_SERIES
})
def rule_rsi(close, window):
    return ta.momentum.rsi(close, window=window)


@register_rule({
    'params': [
        [Ret.CLOSE_PX, Ret.SMA_SERIES],
        [Ret.SMA_SERIES, Ret.SMA_SERIES],
    ],
    'returns': Ret.BOOL_SERIES
})
def rule_crossed(x, y):
    return np.sign(x - y).diff().abs() > 0


@register_rule({
    'params': [
        [Ret.ZH_SERIES, Ret.ZH_NUM],
        # [Ret.MHH_SERIES, Ret.MHH_NUM],
        [Ret.CLOSE_PX, Ret.SMA_SERIES],
        [Ret.SMA_SERIES, Ret.SMA_SERIES],
    ],
    'returns': Ret.BOOL_SERIES
})
def rule_below(x, y):
    return x < y


@register_rule({
    'params': [
        [Ret.ZH_SERIES, Ret.ZH_NUM],
        # [Ret.MHH_SERIES, Ret.MHH_NUM],
        [Ret.CLOSE_PX, Ret.SMA_SERIES],
        [Ret.SMA_SERIES, Ret.SMA_SERIES],
    ],
    'returns': Ret.BOOL_SERIES
})
def rule_above(x, y):
    return x > y


@register_rule({
    'params': [[Ret.BOOL_SERIES, Ret.BOOL_SERIES]],
    'returns': Ret.BOOL_SERIES
})
def rule_and(rule1, rule2):
    return rule1 & rule2


def _get_next_function(return_type: Ret) -> str:
    candidates = RULES_BY_RETURN_TYPE[return_type]
    return random.choice(candidates)


class RuleGenerator:
    def __init__(self, generic: bool = True):
        self.generic = generic

    def _render_function(self, next_func):
        next_params = self._get_params_for_function(next_func)
        if next_func == 'rule_and':
            return f"({next_params[0]} & {next_params[1]})"
        elif next_func == 'rule_below':
            return f"({next_params[0]} < {next_params[1]})"
        elif next_func == 'rule_above':
            return f"({next_params[0]} > {next_params[1]})"

        return f"{next_func.replace('rule_', '')}({', '.join(next_params)})"

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
                    if self.generic:
                        res.append('<CN>')
                    else:
                        res.append(str(self.generate_candles()))
                elif param_type == Ret.ZH_NUM:
                    if self.generic:
                        res.append('<ZHN>')
                    else:
                        res.append(str(round(random.random(), 2)))
                elif param_type == Ret.MHH_NUM:
                    if self.generic:
                        res.append('<MHHN>')
                    else:
                        res.append(str(self.generate_candles()))
                else:
                    raise NotImplementedError(param_type)
                continue
            res.append(self._render_function(next_func))

        return res

    @staticmethod
    def generate_candles():
        return random.randrange(10, 20)

    def generate(self, seed) -> str:
        random.seed(seed)
        func = _get_next_function(Ret.BOOL_SERIES)
        return self._render_function(func)
