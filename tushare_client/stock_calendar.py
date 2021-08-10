from common.utils import *
from tushare_client.base import *


class StockCalendar(AbstractDataRetriever):
    def __init__(self):
        super().__init__('stock_calendar')

    def _full(self, **kwargs):
        df_sse = pro.trade_cal(exchange='SSE')
        df_szse = pro.trade_cal(exchange='SZSE')
        df_shfe = self.__get_delta_stock_calendar('SHFE')
        df_dce = self.__get_delta_stock_calendar('DCE')
        df_cffex = self.__get_delta_stock_calendar('CFFEX')
        df_czce = self.__get_delta_stock_calendar('CZCE')
        df_ine = self.__get_delta_stock_calendar('INE')

        df = pd.concat([df_sse, df_szse, df_shfe, df_dce, df_cffex, df_czce, df_ine], ignore_index=True)
        df['week_day'] = df.apply(lambda x: parse_datetime(x['cal_date']).weekday() + 1, axis=1)

        return df

    def _delta(self, **kwargs):
        df_sse = self.__get_delta_stock_calendar('SSE')
        df_szse = self.__get_delta_stock_calendar('SZSE')
        df_shfe = self.__get_delta_stock_calendar('SHFE')
        df_dce = self.__get_delta_stock_calendar('DCE')
        df_cffex = self.__get_delta_stock_calendar('CFFEX')
        df_czce = self.__get_delta_stock_calendar('CZCE')
        df_ine = self.__get_delta_stock_calendar('INE')

        df = pd.concat([df_sse, df_szse, df_shfe, df_dce, df_cffex, df_czce, df_ine], ignore_index=True)
        if not df.empty:
            df['week_day'] = df.apply(lambda x: parse_datetime(x['cal_date']).weekday() + 1, axis=1)

        return df

    def __get_delta_stock_calendar(self, exchange):
        df_origin = self.query(fields='max(cal_date)', where=f'exchange=\'{exchange}\'')
        if df_origin.empty:
            max_cal_date = '19700101'
        else:
            max_cal_date = df_origin.iat[0, 0] or '19700101'

        return pro.trade_cal(exchange=exchange, start_date=tomorrow(max_cal_date))


if __name__ == '__main__':
    StockCalendar().retrieve()
