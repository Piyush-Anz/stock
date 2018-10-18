#############################################
# Author: Piyush Bijwal
# Comment: 
#############################################



import requests
import datetime
import time
import glob
import pandas as pd
import os
import logprint as lg
import db_postgres_access as db
import numpy as np


t_str_qry="with t_sql as(SELECT exchange, stock, timestamp, high, low,        (high-low) daily_range,       open, close,       lag(close) OVER(Partition BY stock order by timestamp) as prev_close,       volume,       AVG(volume) OVER(ORDER BY stock, timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_volume from (select distinct exchange, stock, timestamp, open, high, low, close, volume from stg_stock_alphavantage) sa) select exchange, stock, timestamp, high, low, daily_range,     open, close,        prev_close,       ((close-prev_close)*100/(case prev_close when 0 then 1 else prev_close END)) as per_growth,       (ln(case close when 0 then 1 else close end)-ln(case prev_close when 0 then 1 else prev_close end) )log_return,       volume,       avg_volume       from t_sql"
df_stock=db.fun_execreq(t_str_qry,'', 'Q')

i_tab_nme='t_stock_alphavantage'
i_action='T'
df_stock=''
db.fun_execreq(df_stock,i_tab_nme, i_action)

i_action='IDF'
db.fun_execreq(df_stock,i_tab_nme, i_action)


