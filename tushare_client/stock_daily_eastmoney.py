import json
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from common.utils import *
from tushare_client.base import *
from tushare_client.stock_basic import StockBasic
from tushare_client.stock_calendar import StockCalendar


class StockDaily(AbstractDataRetriever):
    def __init__(self):
        super().__init__('stock_daily_eastmoney')

    def _full(self, **kwargs):
        self.__get_stock_daily()

        return None

    def _delta(self, **kwargs):
        df_origin = self.query(fields='max(trade_date)')
        if df_origin.empty or df_origin.iat[0, 0] is None:
            self.__get_stock_daily()
        else:
            self.__get_stock_daily(tomorrow(df_origin.iat[0, 0]))

        return None

    def __get_stock_daily(self, start_date='19700101', max_worker=multiprocessing.cpu_count() * 2):
        df_stock_basic = StockBasic().query(
            fields='ts_code',
            where=f'list_status!=\'D\' and ts_code not in (select ts_code from {self.table_name} where trade_date>=\'{start_date}\')',
            order_by='ts_code')

        with ThreadPoolExecutor(max_worker) as executor:
            future_to_date = \
                {executor.submit(self.__get_stock_daily_internal, ts_code=row['ts_code'],
                                 trade_date=start_date): row
                 for index, row in df_stock_basic.iterrows()}
            for future in as_completed(future_to_date):
                row = future_to_date[future]
                try:
                    data = future.result()
                except Exception as ex:
                    self.logger.error(f"failed to retrieve {row['ts_code']}")
                    self.logger.exception(ex)

    def __convert_ts_code(self, ts_code: str):
        word_list = ts_code.split('.')
        if word_list[1] == 'SZ':
            return '0.' + word_list[0]
        else:
            return '1.' + word_list[0]

    def __get_stock_daily_internal(self, ts_code, trade_date):
        secid = self.__convert_ts_code(ts_code)
        url_easymoney = f'http://push2his.eastmoney.com/api/qt/stock/kline/get?fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&beg=0&end=20500101&secid={secid}&klt=101&fqt=0'
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'http://www.iwencai.com'
        }
        r = requests.get(url_easymoney, headers=headers)
        json_value = json.loads(r.content.decode())
        origin_data = json_value['data']['klines']

        actual_data = []
        for index, data in enumerate(origin_data):
            split_data = data.split(sep=",")
            current_date = split_data[0].replace('-', '')
            if current_date >= trade_date:
                split_data.insert(0, ts_code)
                split_data[1] = current_date
                actual_data.append(split_data)

        df = pd.DataFrame(data=actual_data,
                          columns=['ts_code', 'trade_date', 'open', 'close', 'high', 'low', 'vol', 'amount', 'amplitude',
                                   'pct_chg', 'change',
                                   'turnover'])

        self._save(df)


if __name__ == '__main__':
    StockDaily().retrieve()
# 23:04:11