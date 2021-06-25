from datetime import datetime

import backtrader as bt
import pandas as pd
from common.mysqlapi import *


def prepare():
    df_daily = pd.read_sql_query(
        "SELECT * FROM tushare.new_view where ts_code='000001.SZ' and trade_date >= '20190101' order by trade_date",
        engine_ts)

    df_daily.index = pd.to_datetime(df_daily['trade_date'])
    df_daily['openinterest'] = 0
    df_daily = df_daily[['open', 'high', 'low', 'close', 'volume', 'openinterest']]

    return df_daily


class MyStrategy(bt.Strategy):
    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print('%s %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.order = None

        # self.wma = bt.talib.WMA(self.dataclose, timeperiod=15)
        self.MACDHIST = bt.talib.MACD(self.dataclose, fastperiod=12, slowperiod=20, signalperiod=9).macdhist

    def next(self):
        if self.order:
            print('waiting for the deal')

        if not self.position:
            if self.MACDHIST[0] > 0:
                print('buy', self.MACDHIST[0])
                self.order = self.buy(size=100)
        else:
            if self.MACDHIST[0] < 0:
                print('sell', self.MACDHIST[0])
                self.order = self.sell(size=100)

    def notify_order(self, order):
        if order.status == order.Submitted:
            self.log("order is submitted...")
        elif order.status == order.Accepted:
            self.log("order is accepted...")
        elif order.status == order.Completed:
            if order.isbuy():
                self.log(f"executing buy: {order.executed.price}")
            elif order.issell():
                self.log(f"executing sell: {order.executed.price}")

        self.order = None


k_line_data = prepare()
data = bt.feeds.PandasData(dataname=k_line_data, fromdate=datetime(2021, 1, 1), todate=(datetime(2021, 6, 23)))

back_trader = bt.Cerebro()
back_trader.adddata(data)
back_trader.addstrategy(MyStrategy)

startCash = 100000
back_trader.broker.setcash(startCash)
back_trader.broker.setcommission(commission=0.0002)

back_trader.run()

print(back_trader.broker.getvalue())

# back_trader.plot(style='bar')