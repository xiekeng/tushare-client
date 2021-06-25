from tushare_client.stock_basic import StockBasic
from tushare_client.stock_calendar import StockCalendar


if __name__ == '__main__':
    StockBasic().retrieve()
    StockCalendar().query()
