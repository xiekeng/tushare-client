import json

import pandas as pd
import requests
from pandas.io.sql import SQLDatabase
from sqlalchemy import text

from tushare_client.base import AbstractDataRetriever
from common.utils import *
from common.mysqlapi import *
from tushare_client.stock_calendar import StockCalendar


class ConvertibleBond(AbstractDataRetriever):
    def __init__(self):
        super().__init__('convertible_bond', 'replace')

    def _full(self, **kwargs):
        df_cal_date = StockCalendar().query(fields='distinct cal_date',
                                            where=f'is_open=\'1\' and cal_date = \'{today()}\'')
        if df_cal_date is None or df_cal_date.empty:
            return None

        json_value = json.loads(self._get_data_list())
        data = [value['cell'] for value in json_value['rows']]

        columns = ['bond_id', 'bond_nm', 'stock_id', 'convert_price', 'convert_price_valid_from', 'turnover_rt',
                   'rating_cd', 'issuer_rating_cd', 'guarantor', 'convert_value', 'premium_rt', 'ytm_rt', 'price',
                   'volume', 'date']
        df = pd.DataFrame(columns=columns)

        for i in range(0, len(data)):
            data[i] = {key: value for key, value in data[i].items() if key in columns}
            data[i]['date'] = today()
            df = df.append([data[i]], ignore_index=True)

        return df

    def _delta(self, **kwargs):
        return self._full(**kwargs)

    def _replace(self, df):
        db = SQLDatabase(engine_ts)
        with db.run_transaction() as conn:
            if self._initialized():
                stmt_delete = text(f"delete from {self.table_name} where date = '{today()}'")
                conn.execute(stmt_delete)

            df.to_sql(self.table_name, conn, index=False, if_exists='append', chunksize=5000)

    @staticmethod
    def _get_data_list():
        jisilu_url = f'https://www.jisilu.cn/data/cbnew/cb_list/?___jsl=LST___t=&fprice=&tprice=&curr_iss_amt=&volume=&svolume=&premium_rt=&ytm_rt=&rating_cd=&is_search=N&market_cd%5B%5D=shmb&market_cd%5B%5D=shkc&market_cd%5B%5D=szmb&market_cd%5B%5D=szcy&btype=&listed=Y&qflag=N&sw_cd=&bond_ids=&rp=50'
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-Requested-With': 'XMLHttpRequest',
            'Origin': 'https://www.jisilu.cn',
            'Referer': 'https://www.jisilu.cn/data/cbnew/'
        }

        r = requests.post(jisilu_url, headers=headers)
        return r.content.decode()


if __name__ == '__main__':
    ConvertibleBond().retrieve()
