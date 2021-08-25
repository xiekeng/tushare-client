import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser

import pandas as pd
import requests

from common.utils import *
from tushare_client.base import AbstractDataRetriever
from tushare_client.stock_calendar import StockCalendar


class CzceDaily(AbstractDataRetriever):
    def __init__(self):
        super().__init__('futures_czce_daily')

    def _full(self, **kwargs):
        self._get_data_list('20110101', today())

    def _delta(self, **kwargs):
        df_origin = self.query(fields='max(trade_date)')
        if df_origin.empty or df_origin.iat[0, 0] is None:
            self._get_data_list('20110101', today())
        else:
            self._get_data_list(df_origin.iat[0, 0], today())

    def _get_data_list(self, start_date, end_date, max_worker=multiprocessing.cpu_count() * 2):
        df_cal_date = StockCalendar().query(
            fields='cal_date',
            where=f'`exchange`=\'czce\' and is_open=\'1\' and cal_date >=\'{start_date}\' and cal_date <= \'{end_date}\'',
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
        if trade_date > '20151111':
            czce_url = f'http://www.czce.com.cn/cn/DFSStaticFiles/Future/{trade_date[0:4]}/{trade_date}/FutureDataDaily.htm'
        else:
            czce_url = f'http://www.czce.com.cn/cn/exchange/{trade_date[0:4]}/datadaily/{trade_date}.htm'
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2'
        }
        r = requests.get(czce_url, headers=headers)

        data_list = []
        columns = ['instrumentid', 'presettlement', 'open', 'high', 'low', 'close', 'settlement', 'zd1_chg', 'zd2_chg',
                   'volume', 'openinterest', 'openinterestchg', 'turnover', 'deliveryprice']

        parser = CzceHtmlParser()
        parser.feed(r.content.decode())
        data_origin = parser.data

        del data_origin[0]
        for item in data_origin:
            if item[0] != '小计' and item[0] != '总计':
                data_list.append(item)

            if len(item) == 13:
                item.append('')

        df = pd.DataFrame(data=data_list, columns=columns)
        df['trade_date'] = trade_date
        self._save(df)


class CzceHtmlParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_table = False
        self.in_tr = False
        self.in_td = False
        self.last_tag = None
        self.data = []

    def handle_starttag(self, tag, attrs):
        self.last_tag = tag
        if tag == 'table' and (('id', 'senfe') in attrs or ('id', 'tab1') in attrs):
            self.in_table = True

        if self.in_table and tag == 'tr':
            self.data.append([])
            self.in_tr = True

        if self.in_tr and tag == 'td':
            self.in_td = True

    def handle_endtag(self, tag):
        if self.in_table and tag == 'table':
            self.in_table = False

        if self.in_tr and tag == 'tr':
            self.in_tr = False

        if self.in_tr and tag == 'td':
            self.in_td = False

    def handle_data(self, data):
        if self.in_tr and self.in_td:
            self.data[-1].append(data.strip().replace(',', ''))

    def error(self, message):
        print(message)


if __name__ == '__main__':
    CzceDaily().retrieve()
