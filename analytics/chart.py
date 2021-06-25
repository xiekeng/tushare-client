import copy

import mplfinance as mpf
import numpy as np
import pandas as pd
import talib
from talib import MA_Type

from common.mysqlapi import *


class AbstractChart(object):
    def __init__(self, ts_code, start_date, end_date):
        self._data = self.get_data(ts_code, start_date, end_date)

    def get_data(self, ts_code, start_date, end_date):
        df_qfq = pd.read_sql_query(
            f"SELECT * "
            f"FROM view_qfq "
            f"where ts_code='{ts_code}' "
            f"and trade_date >= \'{start_date if start_date is not None else '19700101'}\' "
            f"and trade_date < \'{end_date if end_date is not None else '20991231'}\' "
            f"order by trade_date",
            engine_ts)
        df_qfq.index = pd.to_datetime(df_qfq['trade_date'])
        df_qfq.drop(columns=['trade_date'], inplace=True)

        return df_qfq[['open', 'high', 'low', 'close', 'volume']]

    def _get_plots(self):
        pass

    def _get_default_arguments(self, plots=None):
        arguments = {
            'type': 'candle',
            'style': mpf.make_mpf_style(
                facecolor='black',
                marketcolors=mpf.make_marketcolors(up='red', down='green', inherit=True),
                gridaxis='horizontal',
                gridstyle='--',
                figcolor='(0.82, 0.83, 0.85)',
                gridcolor='(0.82, 0.83, 0.85)',
                y_on_right=False),
            'figratio': (1920, 1080),
            'figscale': 2,
            'volume': True
        }

        addplot = plots if plots is not None else self._get_plots()
        if addplot is not None:
            arguments['addplot'] = addplot

        return arguments

    def show(self, *args, **kwargs):
        arguments = self._get_default_arguments()
        mpf.plot(self._data, **arguments)


class BollChart(AbstractChart):
    def __init__(self, ts_code, start_date, end_date):
        super().__init__(ts_code, start_date, end_date)

    def _get_plots(self):
        upper, middle, lower = talib.BBANDS(self._data.close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=MA_Type.SMA)

        add_plot = [
            mpf.make_addplot(upper, color='y'),
            mpf.make_addplot(middle, color='w'),
            mpf.make_addplot(lower, color='m')
        ]

        return add_plot


class MacdChart(AbstractChart):
    def __init__(self, ts_code, start_date, end_date):
        super().__init__(ts_code, start_date, end_date)

    def _get_plots(self):
        DIFF, DEA, MACDHIST = talib.MACD(self._data.close, fastperiod=12, slowperiod=20, signalperiod=9)

        ORIGIN_MACD_HIST_UP = MACDHIST * 2
        ORIGIN_MACD_HIST_UP[ORIGIN_MACD_HIST_UP < 0] = 0

        ORIGIN_MACD_HIST_DOWN = MACDHIST * 2
        ORIGIN_MACD_HIST_DOWN[ORIGIN_MACD_HIST_DOWN >= 0] = 0

        signals = self.__signal_all_fn(MACDHIST)
        signal_buys = self.__signal_buy_fn(signals)
        signal_sells = self.__signal_sell_fn(signals)

        add_plot = [
            mpf.make_addplot(DIFF, panel=2, color='fuchsia', secondary_y=True),
            mpf.make_addplot(DEA, panel=2, color='w', secondary_y=True),
            mpf.make_addplot(ORIGIN_MACD_HIST_UP, type='bar', panel=2, color='r', secondary_y=True),
            mpf.make_addplot(ORIGIN_MACD_HIST_DOWN, type='bar', panel=2, color='cyan', secondary_y=True),
            mpf.make_addplot(signal_buys, type='scatter', markersize=50, marker='^', color='m'),
            mpf.make_addplot(signal_sells, type='scatter', markersize=50, marker='v', color='yellow')
        ]

        return add_plot

    def __signal_all_fn(self, MACDHIST):
        signal_tip = np.where(MACDHIST > 0, 1, -1)

        df_new = pd.DataFrame(signal_tip, self._data.index)

        signal = np.sign(df_new - df_new.shift(1))
        return signal

    def __signal_buy_fn(self, signals):
        temp = copy.deepcopy(signals)
        temp[temp <= 0] = None
        signal_buy = []

        min = self._data['low'].min()
        max = self._data['high'].max()

        padding = (max - min) / 25

        for index, row in temp.iterrows():
            if row[0] > 0:
                signal_buy.append(self._data[self._data.index == index]['low'] - padding)
            else:
                signal_buy.append(np.nan)

        return np.array(signal_buy, dtype=object)

    def __signal_sell_fn(self, signals):
        temp = copy.deepcopy(signals)
        temp[temp >= 0] = None
        signal_sell = []

        min = self._data['low'].min()
        max = self._data['high'].max()

        padding = (max - min) / 25

        for index, row in temp.iterrows():
            if row[0] < 0:
                signal_sell.append(self._data[self._data.index == index]['high'] + padding)
            else:
                signal_sell.append(np.nan)

        return np.array(signal_sell, dtype=object)


if __name__ == '__main__':
    MacdChart('000001.SZ', '20210101', '20210631').show()
    BollChart('000001.SZ', '20210101', '20210631').show()