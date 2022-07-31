import numpy as np
from arch import arch_model


def get_simple_volatility(data):
    returns = data.pct_change().fillna(0)
    return returns.rolling(window=252).std()*np.sqrt(252)


def get_garch_volatility(data):
    data = data.pct_change().fillna(0) * 100
    am = arch_model(data, mean='Zero', vol='GARCH')
    res = am.fit(disp="off")
    
    return res.conditional_volatility * 16 / 100

