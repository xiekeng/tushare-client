import pandas as pd

from common.mysqlapi import *
from common.utils import *

count = 3

ref_date = pd.read_sql_query(
    f"select distinct cal_date from stock_calendar where cal_date <= '{today()}' and is_open = '1' order by cal_date desc limit {count - 1}, 1",
    engine_ts)
print(ref_date.iloc[0, 0])

date_list = pd.read_sql_query(
    f"select distinct cal_date from stock_calendar where cal_date <= '{today()}' and is_open = '1' order by cal_date desc limit 0, 30",
    engine_ts)
print(date_list['cal_date'])

list_ts_codes = pd.read_sql_query(f"select ts_code from stock_basic where list_status != 'L' or name like '%%ST%%' order by ts_code", engine_ts)
print(list_ts_codes)


def count(trade_date):
    cal_dates = date_list['cal_date'].to_list()
    if trade_date in cal_dates:
        return cal_dates.index(trade_date)


raw_stocks_list = pd.read_sql_query(f"select * from (select ts_code, max(trade_date) trade_date from stock_daily where `change` <= 0 group by ts_code) t where trade_date < '{ref_date.iloc[0, 0]}' and trade_date > '20210101'", engine_ts)
raw_stocks_list['count'] = raw_stocks_list['trade_date'].apply(count)
result = raw_stocks_list[~raw_stocks_list['ts_code'].isin(list_ts_codes['ts_code'])]
result.to_csv('../datafiles/raw_stocks_list')
print(result.groupby('count').count())
