from base import *
from stock_calendar import StockCalendar


class StockAdjFactor(AbstractDataRetriever):
    def __init__(self):
        super().__init__('stock_adj_factor')

    def _full(self, **kwargs):
        df_cal_date = StockCalendar().query(
            fields='distinct cal_date', where=f'is_open=\'1\' and cal_date < \'{tomorrow()}\'', order_by='cal_date')
        
        df = pro.adj_factor(ts_code='000001.SZ', trade_date='')
        print(df)

    def _delta(self, **kwargs):
        df_origin = self.query(fields='max(trade_date)')
        if df_origin.empty:
            max_trade_date = '19810202'
        else:
            max_trade_date = df_origin.iat[0, 0]

        return pro.trade_cal(exchange=exchange, start_date=tomorrow(max_cal_date))


if __name__ == '__main__':
    StockAdjFactor().retrieve()