import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed

from base import *
from stock_calendar import StockCalendar


class StockDaily(AbstractDataRetriever):
    def __init__(self):
        super().__init__('stock_daily')

    def _full(self, **kwargs):
        self.__get_stock_daily()

        return None

    def _delta(self, **kwargs):
        df_origin = self.query(fields='max(trade_date)')
        if df_origin.empty:
            self.__get_stock_daily()
        else:
            self.__get_stock_daily(tomorrow(df_origin.iat[0, 0]))

        return None

    def __get_stock_daily(self, start_date='19810202', max_worker=multiprocessing.cpu_count() * 2):
        df_cal_date = StockCalendar().query(
            fields='distinct cal_date',
            where=f'is_open=\'1\' and cal_date >=\'{start_date}\' and cal_date < \'{tomorrow()}\'',
            order_by='cal_date')

        with ThreadPoolExecutor(max_worker) as executor:
            future_to_date = \
                {executor.submit(self.__get_stock_daily_internal, ts_code='', trade_date=row['cal_date']): row
                 for index, row in df_cal_date.iterrows()}
            for future in as_completed(future_to_date):
                row = future_to_date[future]
                try:
                    data = future.result()
                except Exception as ex:
                    self.logger.exception(ex)

    def __get_stock_daily_internal(self, ts_code, trade_date):
        df = pro.daily(ts_code=ts_code, trade_date=trade_date)
        self.logger.info(f'stock daily[{ts_code}, {trade_date}], length {len(df)}')
        self._save(df)


if __name__ == '__main__':
    StockDaily().retrieve()