from sunflower import data_utils


data_utils.initialize_ticker_for_database('AAPL')


df = data_utils.pull_ticker_history_from_yahoo_finance('AAPL')


print()