import math
import backtrader as bt
import datetime
from talib import MA_Type
import numpy as np

class BaseStratedy(bt.Strategy):
    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print('%s %s' % (dt.isoformat(), txt))


class MacdStrategy(BaseStratedy):
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


class KdjStrategy(BaseStratedy):
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


class SmaStrategy(BaseStratedy):
    ts_code = '000001.SZ'
    fromdate = datetime.date(2005, 1, 1)
    todate = datetime.date(2006, 7, 18)

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.dataopen = self.datas[0].open
        self.selldate = None
        self.buylock = False

        # ema5 = bt.talib.EMA(self.datas[0], timeperiod=2, price='close')
        sma2 = bt.talib.SMA(self.dataclose, timeperiod=2)
        self.fastline = bt.talib.EMA(sma2, timeperiod=20)
        self.slowline = bt.talib.SMA(sma2, timeperiod=60)
        # self.baseline = bt.talib.TEMA(self.dataclose, timeperiod=60)

        # self.ema5 = bt.talib.EMA(self.datas[0], timeperiod=5, price='close')
        # self.sma10 = bt.talib.EMA(self.datas[0], timeperiod=10, price='close')
        # self.sma20 = bt.talib.EMA(self.datas[0], timeperiod=20, price='close')
        # self.tema40 = bt.talib.TEMA(self.datas[0], timeperiod=40, price='close')

        # self.bbands = bt.talib.BBANDS(self.dataclose, timeperiod=20, nbdevup=1.0, nbdevdn=1.0, matype=MA_Type.TEMA)

    def next(self):
        rate = self._current_rate()
        self.log(f'rate: {rate}, fastline[0]: {self.fastline[0]}, slowline[0]: {self.slowline[0]}')
        price = max(self.dataclose[0], self.dataopen[1]) if self.dataopen.get_idx() < len(self.dataopen) - 1 else self.dataclose[0]

        if rate < 50 and self.slowline[0] < self.fastline[0]:
            self.order = self.buy(size=100 * (math.floor(self.broker.getcash() / round(price, 2) / 100) - 1))
        elif rate > 170 and self.slowline[0] > self.fastline[0]:
            self.order = self.sell(size=self.position.size)

        pass

    def notify_order(self, order):
        if order.status == order.Completed:
            if order.isbuy():
                self.log(f"executing buy: {order.executed.price} * {order.executed.size} = {order.executed.price * order.executed.size}")
            elif order.issell():
                self.log(f"executing sell: {order.executed.price} * {order.executed.size}")
        elif order.status == order.Margin:
              self.log('status is margin:')
        #     self.log(order)

    def _current_rate(self):
        size = 244
        index = self.dataclose.get_idx()
        if index < size:
            return size
        sample = self.dataclose.get(size=size).tolist()
        sample.sort()
        rate = sample.index(self.dataclose[0])
        return rate

