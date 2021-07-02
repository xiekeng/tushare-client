import pandas as pd

from tushare_client.base import AbstractDataRetriever


class IndexSsec(AbstractDataRetriever):
    def __init__(self):
        super().__init__('stock_ssec')

    def _full(self, **kwargs):
        with open("data/ssec", "r") as f:
            origin_data = eval(f.read())
            # print(origin_data)

        df = pd.DataFrame(
            columns=['trade_date', 'open', 'close', 'high', 'low', 'vol', 'amount', 'amplitude', 'pct_chg', 'change',
                     'turnover'])

        for index, data in enumerate(origin_data):
            result = data.split(sep=",")
            df.loc[index] = result

        return df

    def _delta(self, **kwargs):
        'http://8.push2his.eastmoney.com/api/qt/stock/kline/get?secid=1.000001&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=101&fqt=2&end=20500101&lmt=1'
        return None


if __name__ == '__main__':
    IndexSsec().retrieve()