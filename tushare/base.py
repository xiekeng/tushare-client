from abc import abstractmethod
from functools import partial

import tushare as ts
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import logging
import time
import datetime

LOG_FORMAT = "[%(asctime)s-%(levelname)s-%(thread)d-%(classname)s] %(message)s"
TUSHARE_TOKEN = '0e2a13806471a7a737cec1b72271ddb19158765f4b971621be370df2'
DB_CONN_STR = 'mysql://tushare:pwd123@127.0.0.1:3306/tushare?charset=utf8&use_unicode=1'
DATE_FORMAT = '%Y%m%d'

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

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


class AbstractDataRetriever(object):
    def __init__(self, table_name, if_exists='append'):
        self.table_name = table_name
        self.if_exists = if_exists
        self.logger = self.CustomLogger(extra={'classname': self.__class__.__name__})

    def retrieve(self, **kwargs):
        self.logger.info(f"retrieve: {kwargs}")
        df = None
        ex = None
        retry = 3
        for _ in range(retry):
            try:
                df = self._get_data(**kwargs)
            except Exception as e:
                self.logger.debug("Failed retrieving data:", e)
                if _ == retry - 1:
                    ex = e;
                time.sleep(1)
            else:
                break

        if df is not None:
            self._save(df)

        if ex is not None:
            self.logger.exception(ex)

        self.logger.info(f"completed")

    def _save(self, df):
        df.to_sql(self.table_name, engine_ts, index=False, if_exists=self.if_exists, chunksize=5000)

    def _initialized(self, **kwargs):
        sql = f"select count(*) from information_schema.tables where table_name = '{self.table_name}';"
        df = pd.read_sql_query(sql, engine_ts)
        return df.iat[0,0] > 0

    def _get_data(self, **kwargs):
        if self._initialized(**kwargs):
            self.logger.info(f"_delta: {kwargs}")
            return self._delta(**kwargs)
        else:
            self.logger.info(f"_full: {kwargs}")
            return self._full(**kwargs)

    def query(self, drop_meta=True, **kwargs):
        sql = f"select {kwargs.setdefault('fields', '*')} " \
              f"from {self.table_name} " \
              f"where {kwargs.setdefault('where', '1=1')} " \
              f"{('order by ' + kwargs.get('order_by')) if 'order_by' in kwargs.keys() else '' }"
        self.logger.debug(f"query: {sql}")
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

    class CustomLogger(object):
        def __init__(self, logger=logging.getLogger(), extra={}):
            self.logger = logger
            self.extra = extra

        def __getattr__(self, name):
            return partial(getattr(self.logger, name), extra=self.extra)