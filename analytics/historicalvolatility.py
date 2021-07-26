import multiprocessing
import os

import pandas as pd
import numpy as np
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from common.mysqlapi import *
from common.utils import *

logger = logging.getLogger('historicalvolatility')
mysql_engines = {}
std_close_prices = []

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

def std_close_price(ts_code):
    global annual_std_close_prices, monthly_std_close_prices
    pid = os.getpid()
    engine = mysql_engines.setdefault(pid, create_engine(config.DB_CONN_STR, echo=True))

    annual = pd.read_sql_query(
        f"select trade_date, close from stock_daily where trade_date > '{last_year()}' and ts_code='{ts_code}'", engine)

    monthly = annual[annual['trade_date']>last_month()]

    return np.std(annual['close']), np.std(monthly['close'])


stock_basic_list = pd.read_sql_query(
    f"select ts_code from stock_basic where list_status = 'L' and name not like '%%ST%%' and list_date < {last_year()}", engine_ts)

with ProcessPoolExecutor(multiprocessing.cpu_count()) as executor:
    futures = {executor.submit(std_close_price, ts_code=row[0]) : row[0] for index, row in stock_basic_list.iterrows()}
    for future in as_completed(futures):
        ts_code = futures[future]
        try:
            annual, monthly = future.result()
            std_close_prices.append([ts_code, annual, monthly])
        except Exception as ex:
            logger.exception(ex)
# annual_std_close_prices = {row[0]: std_close_price(row[0], lastyear()) for index, row in stock_basic_list.iterrows()}
df = pd.DataFrame(std_close_prices, columns=['ts_code', 'annual', 'monthly'])
# print(df)

print(df[df['monthly']>df['annual']])
# annual_std_close_prices - monthly_std_close_prices
# print

# 4200  688685.SH   2.038009   2.357660
# 4203  688690.SH   5.462352   5.512829
# 4204  688698.SH   2.032806   2.093397
