import pandas as pd
import matplotlib.pyplot as plt

from common.mysqlapi import *
from common.utils import *

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

cal_date = pd.read_sql_query(
    f"select distinct cal_date from stock_calendar where is_open='1' and cal_date <= {today()} order by cal_date desc limit 0, 10",
    engine_ts)
# print(cal_date.iloc[9, 0])

df_sw_amount = pd.read_sql_query(
    f"select d.trade_date, sw1, sw2, sw3, vol, amount from sw_industry_classification c, stock_daily d where trade_date >= {cal_date.iloc[9, 0]} and c.ts_code=d.ts_code",
    engine_ts)
# print('df_sw_amount', df_sw_amount)

df1 = df_sw_amount.groupby(['sw1', 'sw2', 'sw3', 'trade_date']).sum()
df1['diffs'] = df1.groupby(['sw1', 'sw2', 'sw3'])['amount'].transform(lambda x: x.diff())
df1 = df1.groupby(['trade_date', 'sw1', 'sw2', 'sw3']).sum().sort_values(['trade_date', 'amount'], ascending=[True, False])
df2 = df1.groupby(['trade_date']).head(5)
# df2.to_csv('../datafiles/top10')
print('df2: ', df2)

df3 = df2.reset_index()
df3['sw'] = df3['sw1'] + '-' + df3['sw2'] + '-' + df3['sw3']
swmap = {sw:df3[df3['sw'] == sw][['trade_date', 'amount']].set_index('trade_date') for sw in df3['sw'].unique()}
result = pd.concat(swmap.values(), keys=swmap.keys(), axis=1)

print('result: ', result)

result.plot(marker='o') # kind='bar'
plt.rcParams['font.sans-serif']=['AR PL UMing CN']
plt.show()
