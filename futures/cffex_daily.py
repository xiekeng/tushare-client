import multiprocessing
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from xml.etree.ElementTree import XML

import requests

from common.utils import *
from tushare_client.base import AbstractDataRetriever
from tushare_client.stock_calendar import StockCalendar


class CffexDaily(AbstractDataRetriever):
    def __init__(self):
        super().__init__('futures_cffex_daily')

    def _full(self, **kwargs):
        self._get_data_list('20110101', today())

    def _delta(self, **kwargs):
        df_origin = self.query(fields='max(tradingday)')
        if df_origin.empty or df_origin.iat[0, 0] is None:
            self._get_data_list('20110101', today())
        else:
            self._get_data_list(df_origin.iat[0, 0], today())

    def _get_data_list(self, start_date, end_date, max_worker=multiprocessing.cpu_count() * 2):
        df_cal_date = StockCalendar().query(
            fields='cal_date',
            where=f'`exchange`=\'cffex\' and is_open=\'1\' and cal_date >=\'{start_date}\' and cal_date <= \'{end_date}\'',
            order_by='cal_date')

        with ThreadPoolExecutor(max_worker) as executor:
            future_to_date = \
                {executor.submit(self._get_daily_data, trade_date=row['cal_date']): row
                 for index, row in df_cal_date.iterrows()}
            for future in as_completed(future_to_date):
                row = future_to_date[future]
                try:
                    data = future.result()
                except Exception as ex:
                    self.logger.error(f"failed to retrieve {row['cal_date']}")
                    self.logger.exception(ex)

    def _get_daily_data(self, trade_date):
        cffex_url = f'http://www.cffex.com.cn/sj/hqsj/rtj/{trade_date[0:6]}/{trade_date[6:8]}/index.xml'
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2'
        }
        r = requests.get(cffex_url, headers=headers)
        xml = XML(r.content.decode())

        data_list = []
        columns = ['instrumentid', 'tradingday', 'openprice', 'highestprice', 'lowestprice', 'closeprice',
                   'preopeninterest', 'openinterest', 'presettlementprice', 'settlementprice', 'turnover', 'volume',
                   'productid', 'expiredate']
        for child in xml:
            data_item = []
            for column in columns:
                data_item.append(child.find(f'./{column}').text)
            data_list.append(data_item)

        df = pd.DataFrame(data=data_list, columns=columns)

        self._save(df)


if __name__ == '__main__':
    CffexDaily().retrieve()
