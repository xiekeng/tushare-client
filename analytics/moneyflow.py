import pandas as pd
import matplotlib.pyplot as plt

from common.mysqlapi import *
from common.utils import *

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

cal_date = pd.read_sql_query(
    f"select distinct cal_date from stock_calendar "
    f"where is_open='1' and cal_date <= {today()} order by cal_date desc limit 0, 10",
    engine_ts)
# print(cal_date.iloc[9, 0])

df_type_amount = pd.read_sql_query(
    f"select d.trade_date, CONCAT_WS('-', sw1, sw2, sw3) type, vol, amount "
    f"from sw_industry_classification c, stock_daily d "
    f"where trade_date >= {cal_date.iloc[9, 0]} and c.ts_code=d.ts_code ",
    # f"select d.trade_date, concept type, vol, amount "
    # f"from stock_concept c, stock_daily d "
    # f"where trade_date >= {cal_date.iloc[9, 0]} and c.ts_code=d.ts_code "
    # f"and concept not in ('标普道琼斯A股', '融资融券', '富时罗素概念', '富时罗素概念股', '深股通', '沪股通', '机构重仓', '转融券标的', 'MSCI概念')",
    engine_ts)
# print('df_type_amount', df_type_amount)

df1 = df_type_amount.groupby(['type', 'trade_date']).sum()
df1['diffs'] = df1.groupby(['type'])['amount'].transform(lambda x: x.diff())
df1 = df1.groupby(['trade_date', 'type']).sum().sort_values(['trade_date', 'amount'], ascending=[True, False])
df2 = df1.groupby(['trade_date']).head(5)
# df2.to_csv('../datafiles/top10')
print('df2: ', df2)

df3 = df2.reset_index()
typemap = {type:df3[df3['type'] == type][['trade_date', 'amount']].set_index('trade_date') for type in df3['type'].unique()}
result = pd.concat(typemap.values(), keys=typemap.keys(), axis=1).sort_index()

print('result: ', result)

result.plot(marker='o') # kind='bar'
plt.rcParams['font.sans-serif']=['AR PL UMing CN']
plt.show()
