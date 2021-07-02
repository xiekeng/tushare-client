from tushare_client.base import *

if __name__ == '__main__':
    df = ts.pro_bar(ts_code='000001.SZ', adj='qfq', start_date='20010101', end_date='20210625')
    print(df)