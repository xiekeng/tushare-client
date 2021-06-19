from abc import abstractmethod

import tushare as ts
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import logging
import time
import datetime

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
TUSHARE_TOKEN = '0e2a13806471a7a737cec1b72271ddb19158765f4b971621be370df2'
DB_CONN_STR = 'mysql://tushare:pwd123@127.0.0.1:3306/tushare?charset=utf8&use_unicode=1'
DATE_FORMAT = '%Y%m%d'

logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

pymysql.install_as_MySQLdb()
engine_ts = create_engine(DB_CONN_STR)


def today(date=None):
    if date is not None:
        return date

    return datetime.date.today().strftime(DATE_FORMAT)


def tomorrow(date=today()):
    current = datetime.datetime.strptime(date, DATE_FORMAT)
    return (current + datetime.timedelta(days=1)).strftime(DATE_FORMAT)


def yesterday(date=today()):
    current = datetime.datetime.strptime(date, DATE_FORMAT)
    return (current + datetime.timedelta(days=-1)).strftime(DATE_FORMAT)


class AbstractDataRetriever:
    def __init__(self, table_name, if_exists='append'):
        self.table_name = table_name
        self.if_exists = if_exists

    def retrieve(self, **kwargs):
        df = None
        for _ in range(3):
            try:
                df = self._get_data(**kwargs)
            except Exception as ex:
                logging.debug("Failed retrieving data:", ex)
                time.sleep(1)
            else:
                break

        if df is not None:
            self._save(df)

    def _save(self, df):
        df.to_sql(self.table_name, engine_ts, index=False, if_exists=self.if_exists, chunksize=5000)

    def _initialized(self, **kwargs):
        sql = f"select count(*) from information_schema.tables where table_name = '{self.table_name}';"
        df = pd.read_sql_query(sql, engine_ts)
        return df.iat[0,0] > 0

    def _get_data(self, **kwargs):
        if self._initialized(**kwargs):
            return self._delta(**kwargs)
        else:
            return self._full(**kwargs)

    def query(self, drop_meta=True, **kwargs):
        sql = f"select {kwargs.setdefault('fields', '*')} " \
              f"from {self.table_name} " \
              f"where {kwargs.setdefault('where', '1=1')} " \
              f"{('order by ' + kwargs.get('order_by')) if 'order_by' in kwargs.keys() else '' }"
        print(sql)
        df = pd.read_sql_query(sql, engine_ts)
        if drop_meta and 'update_time' in df.columns:
            return df.drop(columns=['update_time'])
        else:
            return df

    @abstractmethod
    def _full(self, **kwargs):
        pass

    @abstractmethod
    def _delta(self, **kwargs):
        pass
