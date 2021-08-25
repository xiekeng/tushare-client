import time
from datetime import date

import backtrader as bt
import pandas as pd

from backtrader import cerebro
from common.mysqlapi import *
from stratedy import *


def prepare():
    df_daily = pd.read_sql_query(
        f"SELECT report_date trade_date, open, high, low, close, volume FROM tushare.view_futures_shfe_daily where `name`='铝' order by report_date",
        engine_ts)

    df_daily.index = pd.to_datetime(df_daily['trade_date'])
    df_daily = df_daily[['open', 'high', 'low', 'close', 'volume']]

    return df_daily

stratedy = KdjStrategy
k_line_data = prepare()
data = bt.feeds.PandasData(dataname=k_line_data, fromdate=stratedy.fromdate, todate=stratedy.todate)

back_trader = bt.Cerebro()
back_trader.adddata(data)
back_trader.addstrategy(stratedy)

startCash = 100000
back_trader.broker.setcash(startCash)
back_trader.broker.setcommission(commission=0.00025)
# back_trader.broker.set_coc(True)

back_trader.run()

print(f"from {stratedy.fromdate} to {stratedy.todate}")
datarange = k_line_data.loc[stratedy.fromdate:stratedy.todate]['close']
print("baseline: ", datarange[len(datarange) - 1] / datarange[0])
print("result: ", back_trader.broker.getvalue(), startCash, back_trader.broker.getvalue() / startCash)

back_trader.plot()#style='candlestick')