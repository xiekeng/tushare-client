import threading
import time
from abc import abstractmethod
from functools import partial

import pandas as pd
import tushare as ts
from pandas.io.sql import SQLDatabase
from sqlalchemy import text

from common.customlog import CustomLogger
from common.mysqlapi import *

ts.set_token(config.TUSHARE_TOKEN)


class ThrottleDataApi(object):

    class RequestRecord(object):
        def __init__(self, rate):
            self.request_times = []
            self.throttle_rate = rate
            self.event = threading.Event()
            self.reach_limit = False
            self.lock = threading.RLock()
            self.event.set()

    # func: ([requesttime...], rate, event)
    __request_records = {}

    def __init__(self, api=ts.pro_api()):
        self.api = api
        for key, rate in config.THROTTLE_RATES.items():
            ThrottleDataApi.__request_records[key] = ThrottleDataApi.RequestRecord(rate)

    def __getattr__(self, name):
        self.__allow_request(name)
        return partial(getattr(self.api, name))

    def __allow_request(self, name):
        record = ThrottleDataApi.__request_records.get(name)

        def timer_callback(request_record):
            event.set()
            request_record.reach_limit = False

        if record:
            history = record.request_times
            rate = record.throttle_rate
            event = record.event

            while history and history[-1] <= time.time() - 60:
                history.pop()
            if len(history) >= rate:
                with record.lock:
                    if not record.reach_limit:
                        event.clear()
                        waiting_seconds = 60 + history[-1] - time.time() + 1
                        threading.Timer(waiting_seconds, timer_callback, [record]).start()
                        record.reach_limit = True

            event.wait()
            history.insert(0, time.time())


pro = ThrottleDataApi()


class AbstractDataRetriever(object):
    def __init__(self, table_name, if_exists='append'):
        self.table_name = table_name
        self.if_exists = if_exists
        self.logger = CustomLogger(extra={'classname': self.__class__.__name__})

    def retrieve(self, **kwargs):
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

        if df is not None and not df.empty:
            self._save(df)

        if ex is not None:
            self.logger.exception(ex)

        self.logger.info(f"completed")

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
        df = pd.read_sql_query(sql, engine_ts)
        if drop_meta and 'update_time' in df.columns:
            return df.drop(columns=['update_time'])
        else:
            return df

    def _save(self, df):
        if self.if_exists == 'replace':
            self._replace(df)
        else:
            df.to_sql(self.table_name, engine_ts, index=False, if_exists=self.if_exists, chunksize=5000)

    def _replace(self, df):
        db = SQLDatabase(engine_ts)
        with db.run_transaction() as conn:
            if self._initialized():
                stmt_truncate = text(f"delete from {self.table_name}")
                conn.execute(stmt_truncate)

            df.to_sql(self.table_name, conn, index=False, if_exists='append', chunksize=5000)

    @abstractmethod
    def _full(self, **kwargs):
        pass

    @abstractmethod
    def _delta(self, **kwargs):
        pass
