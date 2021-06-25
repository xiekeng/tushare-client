import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed

from common.utils import *
from tushare_client.base import *
from tushare_client.stock_calendar import StockCalendar


class StockAdjFactor(AbstractDataRetriever):
    def __init__(self):
        super().__init__('stock_adj_factor')

    def _full(self, **kwargs):
        self.__get_adj_factor()

        return None

    def _delta(self, **kwargs):
        df_origin = self.query(fields='max(trade_date)')
        if df_origin.empty:
            self.__get_adj_factor()
        else:
            self.__get_adj_factor(tomorrow(df_origin.iat[0, 0]))

        return None

    def __get_adj_factor(self, start_date='19700101', max_worker=multiprocessing.cpu_count() * 2):
        df_cal_date = StockCalendar().query(
            fields='distinct cal_date',
            where=f'is_open=\'1\' and cal_date >=\'{start_date}\' and cal_date < \'{tomorrow()}\'',
            order_by='cal_date')

        with ThreadPoolExecutor(max_worker) as executor:
            future_to_date = \
                {executor.submit(self.__get_adj_factor_internal, ts_code='', trade_date=row['cal_date']): row
                 for index, row in df_cal_date.iterrows()}
            for future in as_completed(future_to_date):
                row = future_to_date[future]
                try:
                    data = future.result()
                except Exception as ex:
                    self.logger.exception(ex)

    def __get_adj_factor_internal(self, ts_code, trade_date):
        df = pro.adj_factor(ts_code=ts_code, trade_date=trade_date)
        self.logger.info(f'adjustment factor[{ts_code}, {trade_date}], length {len(df)}')
        self._save(df)


if __name__ == '__main__':
    StockAdjFactor().retrieve()