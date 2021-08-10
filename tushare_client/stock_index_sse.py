import json

import pandas as pd
import requests

from common.utils import *
from tushare_client.base import AbstractDataRetriever
from tushare_client.stock_calendar import StockCalendar

stock_index_map = {
    's50': ('stock_index_s50', '000016'),
    'h300': ('stock_index_h300', '000300'),
    'z500': ('stock_index_z500', '000905')
}


class StockIndex(AbstractDataRetriever):
    def __init__(self, index_name):
        stock_index_meta = stock_index_map[index_name]
        super().__init__(stock_index_meta[0])
        self.ts_code = stock_index_meta[1]

    def _full(self, **kwargs):
        return self._get_data_list(50000)

    def _delta(self, **kwargs):
        latest_trade_date = self.query(fields='max(trade_date)')
        cal_days = StockCalendar().query(
            fields='count(cal_date)',
            where=f'is_open=\'1\' and `exchange`=\'SSE\' and cal_date >\'{latest_trade_date.iat[0, 0]}\' and cal_date < \'{tomorrow()}\'')

        limit = cal_days.iat[0, 0]
        if limit > 0:
            return self._get_data_list(cal_days.iat[0, 0])
        else:
            return None

    def _get_data_list(self, limit=1):
        url_easymoney = f'http://8.push2his.eastmoney.com/api/qt/stock/kline/get?secid=1.{self.ts_code}&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=101&fqt=2&end=20500101&lmt={limit}'
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'http://www.iwencai.com'
        }
        r = requests.post(url_easymoney, headers=headers)
        json_value = json.loads(r.content.decode())
        origin_data = json_value['data']['klines']

        df = pd.DataFrame(
            columns=['trade_date', 'open', 'close', 'high', 'low', 'vol', 'amount', 'amplitude', 'pct_chg', 'change',
                     'turnover'])

        for index, data in enumerate(origin_data):
            result = data.split(sep=",")
            df.loc[index] = result

        df['trade_date'] = df.apply(lambda x: x['trade_date'].replace('-', ''), axis=1)

        return df


if __name__ == '__main__':
    StockIndex('s50').retrieve()
    StockIndex('h300').retrieve()
    StockIndex('z500').retrieve()
