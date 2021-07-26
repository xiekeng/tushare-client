import pandas as pd

from common.mysqlapi import *

df_stock_basic = pd.read_sql_query(
    f"SELECT c.*, 'N' as mark FROM sw_industry_classification c, stock_basic b where b.list_status = 'L' and c.ts_code = b.ts_code order by c.ts_code",
    engine_ts)

df_stock_basic.to_csv('../datafiles/stock_basic', index=False)
