import math
from datetime import datetime

import backtrader as bt
import pandas as pd

from common.mysqlapi import *


def prepare():
    df_daily = pd.read_sql_query(
        "SELECT * FROM tushare.view_qfq where ts_code='000001.SZ' and trade_date >= '20010101' order by trade_date",
        engine_ts)

    df_daily.index = pd.to_datetime(df_daily['trade_date'])
    df_daily['openinterest'] = 0
    df_daily = df_daily[['open', 'high', 'low', 'close', 'volume', 'openinterest']]

    return df_daily


class MacdStrategy(bt.Strategy):
    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print('%s %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.order = None
        self.countindex = 0

        # Stochastic = bt.talib.STOCH(self.datas[0].high,
        #                                        self.datas[0].low,
        #                                        self.datas[0].close,
        #                                        fastk_period=9,
        #                                        slowk_period=3,
        #                                        slowd_period=3)
        # self.slowk = Stochastic.slowk
        # self.slowd = Stochastic.slowd
        # self.signal = self.slowk - self.slowd
        self.MACD = bt.talib.MACD(self.dataclose, fastperiod=12, slowperiod=20, signalperiod=9)
        self.dif = self.MACD.macd
        self.dea = self.MACD.macdsignal
        self.signal = self.MACD.macdhist

    def next(self):
        if self.order:
            print('waiting for the deal')

        if not self.position:
            if self.signal[0] > 0 and self.dea > 0:
                # print(f"{round(self.dataclose[0],2)} * {100*(math.floor(self.broker.getcash()/round(self.dataclose[0],2)/100))} = {round(self.dataclose[0],2) * 100*(math.floor(self.broker.getcash()/round(self.dataclose[0],2)/100))} vs {self.broker.getcash()}")
                self.order = self.buy(size=100*(math.floor(self.broker.getcash()/round(self.dataclose[0],2)/100)))
        else:
            if self.signal[0] < 0 and self.dea < 0:
                self.countindex += 1
                if self.countindex == 1:
                    self.order = self.sell(size=self.position.size)
                    self.countindex = 0

    def notify_order(self, order):
        if order.status == order.Completed:
            if order.isbuy():
                self.log(f"executing buy: {order.executed.price} * {order.executed.size} = {order.executed.price * order.executed.size}")
            elif order.issell():
                self.log(f"executing sell: {order.executed.price} * {order.executed.size}")

        # self.log(f"order status: {order.status}")
        self.order = None

class KdjStrategy(bt.Strategy):
    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print('%s %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.order = None
        self.countindex = 0

        Stochastic = bt.talib.STOCH(self.datas[0].high,
                                               self.datas[0].low,
                                               self.datas[0].close,
                                               fastk_period=9,
                                               slowk_period=3,
                                               slowd_period=3)
        self.slowk = Stochastic.slowk
        self.slowd = Stochastic.slowd
        self.signal = self.slowk - self.slowd
        # self.MACD = bt.talib.MACD(self.dataclose, fastperiod=12, slowperiod=20, signalperiod=9)
        # self.dif = self.MACD.macd
        # self.dea = self.MACD.macdsignal
        # self.signal = self.MACD.macdhist

    def next(self):
        if self.order:
            print('waiting for the deal')

        if not self.position:
            if self.signal[0] > 0 and self.slowd > 0:
                # print(f"{round(self.dataclose[0],2)} * {100*(math.floor(self.broker.getcash()/round(self.dataclose[0],2)/100))} = {round(self.dataclose[0],2) * 100*(math.floor(self.broker.getcash()/round(self.dataclose[0],2)/100))} vs {self.broker.getcash()}")
                self.order = self.buy(size=100*(math.floor(self.broker.getcash()/round(self.dataclose[0],2)/100)))
        else:
            if self.signal[0] < 0 and self.slowd < 0:
                self.countindex += 1
                if self.countindex == 1:
                    self.order = self.sell(size=self.position.size)
                    self.countindex = 0

    def notify_order(self, order):
        if order.status == order.Completed:
            if order.isbuy():
                self.log(f"executing buy: {order.executed.price} * {order.executed.size} = {order.executed.price * order.executed.size}")
            elif order.issell():
                self.log(f"executing sell: {order.executed.price} * {order.executed.size}")

        # self.log(f"order status: {order.status}")
        self.order = None

k_line_data = prepare()
data = bt.feeds.PandasData(dataname=k_line_data, fromdate=datetime(2011, 1, 1), todate=(datetime(2021, 6, 30)))

back_trader = bt.Cerebro()
back_trader.adddata(data)
back_trader.addstrategy(MacdStrategy)

startCash = 1000000
back_trader.broker.setcash(startCash)
back_trader.broker.setcommission(commission=0.00025)
# back_trader.broker.set_coc(True)

back_trader.run()

print(back_trader.broker.getvalue())

# back_trader.plot(style='bar')