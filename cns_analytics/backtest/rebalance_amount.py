import pandas as pd


def rebalance_amount(data: pd.Series, amount=100e3, rebalance_days=90)  -> pd.Series:
    """ Keeps the amount of money invested constant.
    Rebalances the portfolio every rebalance_days days.
    """
    result = []

    if data.empty:
        return pd.Series()

    qty = amount / data.iloc[0]
    open_money = -amount

    last_rebalance_date = data.index[0]

    for ts, px in data.iteritems():
        if ts > last_rebalance_date + pd.Timedelta(days=rebalance_days):
            open_money += qty * px
            qty = amount / px
            open_money -= qty * px

            last_rebalance_date = ts
        result.append(open_money + qty * px)

    return pd.Series(result, index=data.index) + amount
