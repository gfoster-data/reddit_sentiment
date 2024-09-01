import pandas as pd
import yfinance as yf

def query_spy(start, end, interval):
    """
    :param start: string format 'YYYY-MM-DD'
    :param end: string format 'YYYY-MM-DD'
    :param interval: string specify data interval '1d', '1m' ...
    :return: df
    """

    spy_history = yf.download('SPY', start=start, end=end, interval=interval)
    spy_history.reset_index(inplace=True)

    columns = [
        'date',
        'open',
        'high',
        'low',
        'close',
        'adj_close',
        'volume'
    ]

    spy_history.columns = columns
    return spy_history

if __name__ == '__main__':

    start = '2024-08-01'
    end = '2024-08-31'
    interval = '1d'

    df = query_spy(start, end, interval)

    pass