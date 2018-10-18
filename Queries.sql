Queries:

piyushbijwal=# \d stg_stock_alphavantage
                                  Table "public.stg_stock_alphavantage"
  Column   |         Type          | Collation | Nullable |                    Default                    
-----------+-----------------------+-----------+----------+-----------------------------------------------
 recordid  | integer               |           | not null | nextval('seq_t_stock_alphavantage'::regclass)
 timestamp | date                  |           |          | 
 open      | numeric               |           |          | 
 high      | numeric               |           |          | 
 low       | numeric               |           |          | 
 close     | numeric               |           |          | 
 volume    | numeric               |           |          | 
 stock     | character varying(10) |           |          | 
 exchange  | character varying(10) |           |          | 
Indexes:
    "stg_stock_alphavantage_pkey" PRIMARY KEY, btree (recordid)


select exchange, stock, timestamp, open, high, low, close, volume from stg_stock_alphavantage
where stock='ANZ'

with t_sql as
(
SELECT exchange, stock, timestamp, high, low, 
       (high-low) daily_range,
       open, close,
       lag(close) OVER(Partition BY stock order by timestamp) as prev_close,
       volume,
       AVG(volume) OVER(ORDER BY stock, timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_volume
from (select distinct exchange, stock, timestamp, open, high, low, close, volume from stg_stock_alphavantage) sa
)
select exchange, stock, timestamp, high, low, daily_range,
       open, close, 
       prev_close,
       ((close-prev_close)*100/(case prev_close when 0 then 1 else prev_close END)) as per_growth,
       volume,
       avg_volume
       from t_sql
 where stock='ANZ'
and timestamp>'2018-09-30'



drop table t_stock_alphavantage;
create table t_stock_alphavantage as
with t_sql as
(
SELECT exchange, stock, timestamp, high, low, 
       (high-low) daily_range,
       open, close,
       lag(close) OVER(Partition BY stock order by timestamp) as prev_close,
       volume,
       AVG(volume) OVER(ORDER BY stock, timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_volume
from (select distinct exchange, stock, timestamp, open, high, low, close, volume from stg_stock_alphavantage) sa
)
select exchange, stock, timestamp, high, low, daily_range,
       open, close, 
       prev_close,
       ((close-prev_close)*100/(case prev_close when 0 then 1 else prev_close END)) as per_growth,
       (ln(case close when 0 then 1 else close end)-ln(case prev_close when 0 then 1 else prev_close end) )log_return,
       volume,
       avg_volume
       from t_sql



