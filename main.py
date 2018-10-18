#############################################
# Author: Piyush Bijwal
# Comment: Main stript
#############################################



import logprint as lg
import pandas as pd
import get_stock_extract as se
import db_postgres_access as db
import time



def fun_insert_stg(t_stock_ex):
		t_stock=t_stock_ex.split('.')[0]
		t_exchange=t_stock_ex.split('.')[1]
#	t_stock_ex=t_stock+'.'+t_exchange
		i_url=se.fun_url_alpha(t_stock_ex)
		r_response=se.fun_download_url(i_url)
		df_stock=se.fun_respone_2_df(r_response)
		df_stock["stock"]=t_stock
		df_stock["exchange"]=t_exchange
		i_tab_nme='stg_stock_alphavantage'
		i_action='IDF'
		db.fun_execreq(df_stock,i_tab_nme, i_action)

def fun_get200asx_stock():
	lg.echo_msg("Inside the function fun_ax_stock")
	t_str_qry="select Exchange,Stock_Name,Stock,Sector from t_top10_200ASX_IdxWt"
	lg.echo_msg("Calling function db.fun_execreq to execute Query:: "+t_str_qry)
	df=db.fun_execreq(t_str_qry,'', 'Q')
	lg.echo_msg(df)
	for t_stock_ex in df['stock']+"."+df['exchange']:
		fun_insert_stg(t_stock_ex)
		time.sleep(15)

def fun_getall_stock():
	lg.echo_msg("Inside the function fun_all_asx_stock")
	t_str_qry="select Exchange,Stock_Name,Stock,Sector from t_all_ASX_stock"
	lg.echo_msg("Calling function db.fun_execreq to execute Query:: "+t_str_qry)
	df=db.fun_execreq(t_str_qry,'', 'Q')
	lg.echo_msg(df)
	for t_stock_ex in df['stock']+"."+df['exchange']:
		fun_insert_stg(t_stock_ex)
		time.sleep(13)

### fun_daily_incr_aggr will analyse the extracted data in stg_stock_alphavantage and compare with already aggregated data to include increment data.
def fun_daily_incr_aggr():
	lg.echo_msg("Inside the function fun_daily_aggr")
	t_str_qry="with t_sql as(SELECT exchange, stock, timestamp, high, low,        (high-low) daily_range,       open, close,       lag(close) OVER(Partition BY stock order by timestamp) as prev_close,       volume,       AVG(volume) OVER(ORDER BY stock, timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_volume from (select distinct exchange, stock, timestamp, open, high, low, close, volume from stg_stock_alphavantage) sa) select exchange, stock, timestamp, high, low, daily_range,     open, close,        prev_close,       ((close-prev_close)*100/(case prev_close when 0 then 1 else prev_close END)) as per_growth,       (ln(case close when 0 then 1 else close end)-ln(case prev_close when 0 then 1 else prev_close end) )log_return,       volume,       avg_volume       from t_sql"
	lg.echo_msg("Calling function db.fun_execreq to execute Query:: "+t_str_qry)
	df_new=db.fun_execreq(t_str_qry,'', 'Q')
	t_str_qry="select exchange, stock, timestamp, high, low, daily_range, open, close, prev_close, per_growth, log_return, volume, avg_volume from t_stock_alphavantage"
	lg.echo_msg("Calling function db.fun_execreq to execute Query:: "+t_str_qry)
	df_table=db.fun_execreq(t_str_qry,'', 'Q')
	lg.echo_msg("Identify the changes")
	df_change=df_new.merge(df_table.assign(flg=1),how='left').loc[lambda x :x['flg'].isnull(),:]
	del df_change['flg']
	lg.echo_msg("New record count"+str(df_change.count()))
	i_tab_nme='t_stock_alphavantage'
	i_action='IDF'
	lg.echo_msg("Appenidng the change data")
	db.fun_execreq(df_change,i_tab_nme, i_action)


def fun_sel_stock(i_stockcode):
	lg.echo_msg("Inside the function fun_sel_stock")
	t_str_qry="select exchange, stock, timestamp, high, low, daily_range, open, close, prev_close, per_growth, log_return, volume, avg_volume from t_stock_alphavantage where stock='"+i_stockcode+"'"
	lg.echo_msg("Calling function db.fun_execreq to execute Query:: "+t_str_qry)
	df=db.fun_execreq(t_str_qry,'', 'Q')
	lg.echo_msg(df)



fun_getall_stock()
fun_daily_incr_aggr()
#fun_sel_stock('NAB')
#fun_get200asx_stock()










