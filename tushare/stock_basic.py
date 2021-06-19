from base import *


class StockBasic(AbstractDataRetriever):
    _fields = 'ts_code,symbol,name,area,industry,market,exchange,list_status,list_date,delist_date,is_hs'

    def __init__(self):
        super().__init__('stock_basic')

    def _full(self, **kwargs):
        df_list = pro.stock_basic(list_status='L', fields=StockBasic._fields)
        df_delist = pro.stock_basic(list_status='D', fields=StockBasic._fields)
        df_pending = pro.stock_basic(list_status='P', fields=StockBasic._fields)

        return pd.concat([df_list, df_delist, df_pending], ignore_index=True)

    def _delta(self, **kwargs):
        df_origin = self.query()
        df_current = self._full(**kwargs)

        df_current = df_current.append(df_origin)
        df_current = df_current.append(df_origin)
        df_diff = df_current.drop_duplicates(['ts_code'], keep=False)

        return df_diff


if __name__ == '__main__':
    StockBasic().retrieve()
