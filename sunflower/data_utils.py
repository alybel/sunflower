import datetime

import pandas_datareader.data as web
from sqlalchemy import create_engine

from . import config


def get_sql_connection(mode=config.mode):
    engine = None
    if mode == 'dev':
        engine = create_engine('mysql+pymysql://user1:sunflower1@localhost/findata')
    elif mode == 'prod':
        pass
    else:
        raise AttributeError('database mode not understood')
    return engine


def pull_ticker_history_from_yahoo_finance(ticker=''):
    start = datetime.datetime(1970, 1, 1)
    end = datetime.datetime.today()
    df = get_data_from_yahoo_finance(ticker=ticker, start=start, end=end)
    return df


def get_data_from_yahoo_finance(ticker='', start=None, end=None):
    if start is None or end is None:
        raise AttributeError('Prove a start and end date')
    return web.DataReader(ticker, 'yahoo', start=start, end=end)


def initialize_ticker_for_database(ticker=''):
    df = pull_ticker_history_from_yahoo_finance(ticker=ticker)
    engine = get_sql_connection()
    df.to_sql(name=ticker, con=engine, if_exists='replace', schema='findata')
