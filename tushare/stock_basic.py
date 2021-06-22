from base import *


class StockBasic(AbstractDataRetriever):
    _fields = 'ts_code,symbol,name,area,industry,market,exchange,list_status,list_date,delist_date,is_hs'

    def __init__(self):
        super().__init__('stock_basic', if_exists='replace')

    def _full(self, **kwargs):
        df_list = pro.stock_basic(list_status='L', fields=StockBasic._fields)
        df_delist = pro.stock_basic(list_status='D', fields=StockBasic._fields)
        df_pending = pro.stock_basic(list_status='P', fields=StockBasic._fields)

        return pd.concat([df_list, df_delist, df_pending], ignore_index=True)

    def _delta(self, **kwargs):
        return self._full(**kwargs)


if __name__ == '__main__':
    StockBasic().retrieve()
