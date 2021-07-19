import time
from datetime import date

import backtrader as bt
import pandas as pd

from backtrader import cerebro
from common.mysqlapi import *
from stratedy import *


def prepare(ts_code):
    df_daily = pd.read_sql_query(
        f"SELECT * FROM tushare.view_qfq where ts_code='{ts_code}' order by trade_date",
        engine_ts)

    df_daily.index = pd.to_datetime(df_daily['trade_date'])
    df_daily = df_daily[['open', 'high', 'low', 'close', 'volume']]

    return df_daily

stratedy = SmaStrategy
k_line_data = prepare(stratedy.ts_code)
data = bt.feeds.PandasData(dataname=k_line_data, fromdate=stratedy.fromdate, todate=stratedy.todate)

back_trader = bt.Cerebro()
back_trader.adddata(data)
back_trader.addstrategy(SmaStrategy)

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