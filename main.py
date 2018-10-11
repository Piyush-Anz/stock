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

def fun_200asx_stock():
	lg.echo_msg("Inside the function fun_ax_stock")
	t_str_qry="select Exchange,Stock_Name,Stock,Sector from t_top10_200ASX_IdxWt"
	lg.echo_msg("Calling function db.fun_execreq to execute Query:: "+t_str_qry)
	df=db.fun_execreq(t_str_qry,'', 'Q')
	lg.echo_msg(df)
	for t_stock_ex in df['stock']+"."+df['exchange']:
		fun_insert_stg(t_stock_ex)
		time.sleep(15)


fun_200asx_stock()


