from sunflower import data_utils


data_utils.manage_data()




data_utils.update_ticker('AAPL')


df = data_utils.get_data_from_yahoo_finance(ticker='AAPL', start_date=datetime.datetime(1990, 1, 1), end_date=datetime.datetime(2017, 9, 28))
data_utils.write_ticker_data_to_db(df, 'AAPL')


print(data_utils.get_max_available_date_for_ticker('AAPL'))

print(data_utils.table_exists('AAPL'))


data_utils.initialize_ticker_for_database('AAPL2')


df = data_utils.pull_ticker_history_from_yahoo_finance('AAPL')


print()