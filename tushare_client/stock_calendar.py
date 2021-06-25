from common.utils import *
from tushare_client.base import *


class StockCalendar(AbstractDataRetriever):
    def __init__(self):
        super().__init__('stock_calendar')

    def _full(self, **kwargs):
        df_sse = pro.trade_cal(exchange='SSE')
        df_szse = pro.trade_cal(exchange='SZSE')

        return pd.concat([df_sse, df_szse], ignore_index=True)

    def _delta(self, **kwargs):
        df_sse = self.__get_delta_stock_calendar('SSE')
        df_szse = self.__get_delta_stock_calendar('SZSE')

        return pd.concat([df_sse, df_szse], ignore_index=True)

    def __get_delta_stock_calendar(self, exchange):
        df_origin = self.query(fields='max(cal_date)', where=f'exchange=\'{exchange}\'')
        if df_origin.empty:
            max_cal_date = '19700101'
        else:
            max_cal_date = df_origin.iat[0, 0]

        return pro.trade_cal(exchange=exchange, start_date=tomorrow(max_cal_date))


if __name__ == '__main__':
    StockCalendar().retrieve()
