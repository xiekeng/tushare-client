import json
import math

import pandas as pd
import requests

from tushare_client.base import AbstractDataRetriever
from common.utils import *

default_per_page = 5000


class SwsIndustryClass(AbstractDataRetriever):
    def __init__(self):
        super().__init__('sw_industry_classification', if_exists='replace')

    def _full(self, **kwargs):
        df = pd.DataFrame(
            columns=['ts_code', 'name', 'sw1', 'sw2', 'sw3'])

        json_value = json.loads(self._get_data_list())
        data = json_value['answer']['components'][0]['data']['datas']
        row_count = json_value['answer']['components'][0]['data']['meta']['extra']['row_count']

        for i in range(1, row_count, default_per_page):
            for record in data:
                if record['所属申万行业'] is not None:
                    sws = record['所属申万行业'].split('--')
                else:
                    sws = ['', '', '']
                df = df.append(
                    [{'ts_code': record['股票代码'], 'name': record['股票简称'],
                      'sw1': sws[0], 'sw2': sws[1], 'sw3': sws[2],
                      'pe': record.setdefault(f'市盈率(pe)[{today()}]', None), 'total_share': record.setdefault(f'总股本[{today()}]', None)}],
                    ignore_index=True)

            page = math.floor(i / default_per_page) + 2
            json_value = json.loads(self._get_data_list(page))
            data = json_value['answer']['components'][0]['data']['datas']

        return df

    def _delta(self, **kwargs):
        return self._full(**kwargs)

    def _get_data_list(self, page=1, per_page=default_per_page):
        iwencai_url = f'http://ai.iwencai.com/urp/v7/landing/getDataList?query=%E7%94%B3%E4%B8%87%E8%A1%8C%E4%B8%9A&condition=%5B%7B%22indexName%22%3A%22%E6%89%80%E5%B1%9E%E7%94%B3%E4%B8%87%E8%A1%8C%E4%B8%9A%22%7D%5D&page={page}&perpage={per_page}&comp_id=5762923&uuid=24087'
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'http://www.iwencai.com'
        }
        r = requests.post(iwencai_url, headers=headers)
        return r.content.decode()


if __name__ == '__main__':
    SwsIndustryClass().retrieve()
