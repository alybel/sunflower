import datetime
import time

import pandas_datareader.data as web
from sqlalchemy import create_engine

from . import config


class Init(object):
    def __init__(self):
        self._engine = None

    @property
    def engine(self):
        if self._engine is None:
            self._engine = get_sql_connection()
        return self._engine


init = Init()


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
    df = get_data_from_yahoo_finance(ticker=ticker, start_date=start, end_date=end)
    return df


def get_data_from_yahoo_finance(ticker='', start_date=None, end_date=None):
    if start_date is None or end_date is None:
        raise AttributeError('Prove a start and end date')
    return web.DataReader(ticker, 'yahoo', start=start_date, end=end_date)


def write_ticker_data_to_db(df, ticker):
    df.to_sql(name=ticker, con=init.engine, if_exists='replace', schema=config.findata_schema)


def initialize_ticker_for_database(ticker=''):
    df = pull_ticker_history_from_yahoo_finance(ticker=ticker)
    write_ticker_data_to_db(df=df, ticker=ticker)


def table_exists(ticker=''):
    return init.engine.dialect.has_table(init.engine, ticker)


def execute_sql(sql=''):
    c = init.engine.connect()
    result = c.execute(sql)
    c.close()
    return result


def get_max_available_date_for_ticker(ticker=''):
    if not table_exists(ticker=ticker):
        raise ValueError('Table does not exist')
    sql_str = 'select max(Date) from %s.`%s`;' % (config.findata_schema, ticker)
    res = execute_sql(sql_str)
    return res.first()[0]


def get_min_available_date_for_ticker(ticker=''):
    if not table_exists(ticker=ticker):
        raise ValueError('Table does not exist')
    sql_str = 'select min(Date) from %s.`%s`;' % (config.findata_schema, ticker)
    res = execute_sql(sql_str)
    return res.first()[0]


def drop_latest_entry_for_ticker(ticker=''):
    max_date = get_max_available_date_for_ticker(ticker=ticker)
    sql_str = 'delete from %s.`%s` where Date = "%s";' % (config.findata_schema, ticker, max_date)
    execute_sql(sql_str)


def get_number_rows_per_ticker(ticker=''):
    sql_str = 'select count(*) from %s.`%s`;' % (config.findata_schema, ticker)
    res = execute_sql(sql_str)
    return res.first()[0]

def drop_ticker_table(ticker=''):
    sql_str = 'DROP TABLE `%s`.`%s`;' % (config.findata_schema, ticker)
    res = execute_sql(sql_str)

def update_ticker(ticker=''):
    print(ticker)
    if table_exists(ticker=ticker):
        # drop today and yesterday because prices change intraday
        n_hist = get_number_rows_per_ticker(ticker=ticker)
        if n_hist < 2:
            drop_ticker_table(ticker=ticker)
            return
        for i in range(2):
            drop_latest_entry_for_ticker(ticker=ticker)

        max_date = get_max_available_date_for_ticker(ticker=ticker)
        today = datetime.datetime.today()

        if (today - max_date).days > 0:
            start_date = max_date + datetime.timedelta(days=1)
            end_date = today
            print('load %s data between %s and %s' % (ticker, start_date, end_date))
            df = get_data_from_yahoo_finance(ticker=ticker, start_date=start_date, end_date=end_date)
            df.to_sql(name=ticker, con=init.engine, if_exists='append', schema=config.findata_schema)

    else:
        initialize_ticker_for_database(ticker=ticker)


def manage_data():
    for ticker in config.list_of_tickers:
        update_ticker(ticker)
        time.sleep(10)
