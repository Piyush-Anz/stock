


import logprint as lg
import pandas as pd
import get_stock_extract as se
import db_postgres_access as db
import time

lg.echo_msg("Inside the function fun_ax_stock")
t_str_qry="select distinct stock, timestamp, open, high, low, close,volume from stg_stock_alphavantage where stock='ANZ' order by timestamp"
lg.echo_msg("Calling function db.fun_execreq to execute Query:: "+t_str_qry)
df=db.fun_execreq(t_str_qry,'', 'Q')
df8=df[:17]
df8
df8['prev1'] = df8.groupby(['stock'])['close'].shift(1)
df8['prev2'] = df8.groupby(['stock'])['close'].shift(2)
df8['prev3'] = df8.groupby(['stock'])['close'].shift(3)
df8['prev4'] = df8.groupby(['stock'])['close'].shift(4)
df8['prev5'] = df8.groupby(['stock'])['close'].shift(5)
df8['prev6'] = df8.groupby(['stock'])['close'].shift(6)
df8['prev7'] = df8.groupby(['stock'])['close'].shift(7)
df8
