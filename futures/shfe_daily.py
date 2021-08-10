import json
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

from tushare_client.base import AbstractDataRetriever
from tushare_client.stock_calendar import StockCalendar
from common.utils import *

default_per_page = 5000


class ShfeDaily(AbstractDataRetriever):
    def __init__(self):
        super().__init__('futures_shfe_daily')

    def _full(self, **kwargs):
        self._get_data_list('20110101', today())

    def _delta(self, **kwargs):
        df_origin = self.query(fields='max(report_date)')
        if df_origin.empty or df_origin.iat[0, 0] is None:
            self._get_data_list('20110101', today())
        else:
            self._get_data_list(df_origin.iat[0, 0], today())

    def _get_data_list(self, start_date, end_date, max_worker=multiprocessing.cpu_count() * 2):
        df_cal_date = StockCalendar().query(
            fields='cal_date',
            where=f'`exchange`=\'shfe\' and is_open=\'1\' and cal_date >=\'{start_date}\' and cal_date <= \'{end_date}\'',
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
        shfe_url = f'http://www.shfe.com.cn/data/dailydata/kx/kx{trade_date}.dat'
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2'
        }
        r = requests.post(shfe_url, headers=headers)
        json_value = json.loads(r.content.decode())

        df = pd.DataFrame(
            columns=['productid', 'name', 'deliverymonth', 'open', 'high', 'low', 'close', 'settlement', 'zd1_chg',
                     'zd2_chg', 'volume', 'turnover', 'openinterest', 'openinterestchg', 'report_date'])

        for data in json_value['o_curinstrument']:
            if data['OPENPRICE'] != '':
                if 'TURNOVER' in data:
                    turnover = data['TURNOVER']
                else:
                    turnover = None
                df = df.append(
                    {'productid':data['PRODUCTID'], 'name':data['PRODUCTNAME'],'deliverymonth':data['DELIVERYMONTH'],
                     'open':data['OPENPRICE'], 'high':data['HIGHESTPRICE'], 'low':data['LOWESTPRICE'],
                     'close':data['CLOSEPRICE'],'settlement':data['SETTLEMENTPRICE'],'zd1_chg':data['ZD1_CHG'],
                     'zd2_chg':data['ZD2_CHG'],'volume':data['VOLUME'],  'turnover':turnover,
                     'openinterest':data['OPENINTEREST'],'openinterestchg':data['OPENINTERESTCHG'],
                     'report_date':trade_date},
                    ignore_index=True)

        self._save(df)


if __name__ == '__main__':
    ShfeDaily().retrieve()
