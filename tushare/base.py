import datetime
import logging
import threading
import time
from abc import abstractmethod
from functools import partial

import pandas as pd
import pymysql
import tushare as ts
from sqlalchemy import create_engine

LOG_FORMAT = "[{asctime}-{levelname}-{thread}-{classname}] {message}"
TUSHARE_TOKEN = '0e2a13806471a7a737cec1b72271ddb19158765f4b971621be370df2'
DB_CONN_STR = 'mysql://tushare:pwd123@127.0.0.1:3306/tushare?charset=utf8&use_unicode=1'
DATE_FORMAT = '%Y%m%d'


class CustomFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style='{', validate=True):
        super().__init__(fmt, datefmt, style, validate)

    def formatMessage(self, record):
        return self._fmt.format_map(CustomFormatter.Default(record.__dict__))

    class Default(dict):
        def __missing__(self, key):
            return '{' + key + '}'


def default_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(CustomFormatter(LOG_FORMAT))
    logger.addHandler(console_handler)

    return logger


class CustomLogger(object):
    def __init__(self, logger=default_logger(), extra={}):
        self.logger = logger
        self.extra = extra

    def __getattr__(self, name):
        return partial(getattr(self.logger, name), extra=self.extra)


class ThrottleDataApi(object):
    # func: times/min
    THROTTLE_RATES = {
        'daily': 500,
        'adj_factor': 500
    }

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
        for key, rate in ThrottleDataApi.THROTTLE_RATES.items():
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


ts.set_token(TUSHARE_TOKEN)
pro = ThrottleDataApi()

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
        self.logger = CustomLogger(extra={'classname': self.__class__.__name__})

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
