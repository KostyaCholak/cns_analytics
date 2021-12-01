"""
CNS Analytics

| Package for time series analytics and backtest
| Basic Usage:
|
| Spreads:

.. python::

    from cns_analytics.timeseries import Spread
    spread = Spread('UNIUSDT', '1INCHUSDT')
    await spread.load()

    spread.plot()

|
| Timeseries:

.. python::

    from cns_analytics.timeseries import Timeseries
    ts = Timeseries('UNIUSDT', '1INCHUSDT', 'AAVAUSDT')
    await ts.load()
    print('Volatility for UNIUSDT', ts.get_volatility('UNIUSDT'))

"""

from .timeseries.spread import Spread
from .timeseries import TimeSeries
from .database import DataBase
from .entities import Exchange, Symbol, Duration, DateTime
from .utils.formula import formula_to_ts
from .backtest.fix import get_fix
from .visualization import Image, Animation
