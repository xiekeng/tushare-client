import pandas as pd

from common.customlog import default_logger
from common.mysqlapi import *

pd.set_option('display.max_rows', None)

df_sw_amount = pd.read_sql_query(
    f"select d.trade_date, sw1, sw2, sw3, vol, amount from sw_industry_classification c, stock_daily d where trade_date > '20210710' and c.ts_code=d.ts_code",
    engine_ts)

df1 = df_sw_amount.groupby(['sw1', 'sw2', 'sw3', 'trade_date']).sum()
df1['diffs'] = df1.groupby(['sw1', 'sw2', 'sw3'])['amount'].transform(lambda x: x.diff())
df1 = df1.groupby(['trade_date', 'sw1', 'sw2', 'sw3']).sum().sort_values(['trade_date', 'amount'], ascending=[True, False])
df2 = df1.groupby(['trade_date']).head(5)
print('df2: ', df2)
