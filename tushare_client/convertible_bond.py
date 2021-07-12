import json

import pandas as pd
import requests

from tushare_client.base import AbstractDataRetriever

default_per_page = 5000


class SwsIndustryClass(AbstractDataRetriever):
    def __init__(self):
        super().__init__('convertible_bond')

    def _full(self, **kwargs):
        json_value = json.loads(self._get_data_list())
        data = [value['cell'] for value in json_value['rows']]

        columns = ['bond_id', 'bond_nm', 'stock_id', 'convert_price', 'convert_price_valid_from', 'turnover_rt',
                   'rating_cd', 'issuer_rating_cd', 'guarantor', 'convert_value', 'premium_rt', 'ytm_rt', 'price',
                   'volume']
        df = pd.DataFrame(columns=columns)

        for i in range(0, len(data)):
            data[i] = {key: value for key, value in data[i].items() if key in columns}
            df = df.append([data[i]], ignore_index=True)

        return df

    def _delta(self, **kwargs):
        return self._full(**kwargs)

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
    SwsIndustryClass().retrieve()
