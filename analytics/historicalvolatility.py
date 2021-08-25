import multiprocessing
import os

import pandas as pd
import numpy as np
import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from common.mysqlapi import *
from common.utils import *

logger = logging.getLogger('historicalvolatility')
mysql_engines = {}
std_close_prices = []

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

now = '20210813'


def std_close_price(ts_code):
    pid = os.getpid()
    engine = mysql_engines.setdefault(pid, create_engine(config.DB_CONN_STR, echo=True))

    yearly = pd.read_sql_query(
        f"select trade_date, close from stock_daily where trade_date > '{last_year(now)}' and trade_date <= '{now}' and ts_code='{ts_code}'", engine)
    monthly = yearly[yearly['trade_date']>last_month(now)]
    weekly = yearly[yearly['trade_date']>last_week(now)]

    return np.std(yearly['close']), np.std(monthly['close']), np.std(weekly['close'])


stock_basic_list = pd.read_sql_query(
    f"select ts_code from stock_basic where list_status = 'L' and name not like '%%ST%%'", engine_ts)

with ProcessPoolExecutor(multiprocessing.cpu_count()) as executor:
    futures = {executor.submit(std_close_price, ts_code=row[0]) : row[0] for index, row in stock_basic_list.iterrows()}
    for future in as_completed(futures):
        ts_code = futures[future]
        try:
            yearly, monthly, weekly = future.result()
            std_close_prices.append([ts_code, yearly, monthly, weekly])
        except Exception as ex:
            logger.exception(ex)
# annual_std_close_prices = {row[0]: std_close_price(row[0], lastyear()) for index, row in stock_basic_list.iterrows()}
df = pd.DataFrame(std_close_prices, columns=['ts_code', 'yearly', 'monthly', 'weekly'])
df = df[df['monthly']>df['yearly']]
df = df[df['weekly']>df['yearly']]
print(df)
print(df['ts_code'].to_list())
# annual_std_close_prices - monthly_std_close_prices
# print

# 20210802 ['002125.SZ', '002533.SZ', '002687.SZ', '300041.SZ', '300329.SZ', '300346.SZ', '300648.SZ', '300660.SZ', '300661.SZ', '300693.SZ', '300827.SZ', '600366.SH', '601669.SH', '601998.SH', '603315.SH', '688601.SH', '688663.SH']
# 20210803 ['000507.SZ', '002097.SZ', '002121.SZ', '300041.SZ', '300101.SZ', '300289.SZ', '300329.SZ', '300346.SZ', '300508.SZ', '300656.SZ', '300693.SZ', '300827.SZ', '600888.SH', '601669.SH', '601877.SH', '601998.SH', '603315.SH', '688128.SH', '688663.SH']

