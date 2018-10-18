#############################################
# Author: Piyush Bijwal
# Comment: An independent service to interact with database.
#
#
#############################################


import requests
import datetime
import time
import glob
import pandas as pd
from sqlalchemy import create_engine as ce
import os
import psycopg2
import logprint as lg

def postgres_conn():
    try:
    	t_str='dbname='+os.getenv("db_dbname")+' user='+os.getenv("db_user")+' host='+os.getenv("db_host")+' password='+os.getenv("logpwd")+' connect_timeout=1 '
    	conn = psycopg2.connect(t_str)
        lg.echo_msg('DB Connection test successfull')
#        conn.close()
        return conn
    except:
    	lg.echo_msg('DB Connection test unsuccessfull')
        return False

def fun_insert(df_4db,i_tab_nme):
	try:
		conn=postgres_conn()
		t_str='postgresql://'+os.getenv("db_user")+':'+os.getenv("logpwd")+'@'+os.getenv("db_host")+':'+os.getenv("db_port")+'/'+os.getenv("db_user")
		dbengine = ce(t_str)
		print 'dbengine executed'
		df_4db.head(0).to_sql(i_tab_nme, con=dbengine,if_exists='append')
		print 'head(0) executed'
		df_4db.to_sql(i_tab_nme, con=dbengine,if_exists='append', index=False)
		print 'data insert executed'
		conn.close()
		return True
	except:
		lg.echo_msg('Error in function fun_conndb component IDF')
		return False

def fun_query(df_4db):
		try:
			conn=postgres_conn()
			cur=conn.cursor()
			cur.execute(df_4db)
			colnames = [desc[0] for desc in cur.description]
			fetch_rec = cur.fetchall()
			df= pd.DataFrame(list(fetch_rec),columns=colnames)
			return df
		except:
			lg.echo_msg('Error in function fun_conndb component Q')
			return False

def fun_truncate(i_tab_nme):
		try:
			i_str='Truncate ' + str(i_tab_nme)
			conn=postgres_conn()
			cur=conn.cursor()
			lg.echo_msg('Executing stmt:::'+i_str)
			cur.execute(i_str)
			conn.commit
			conn.close()
			return 0
		except:
			lg.echo_msg('Error in function fun_truncate component T')
			return False


def fun_execreq(df_4db,i_tab_nme, i_action):
	### Make the below string as an environment variable
	if i_action == 'IDF':
		lg.echo_msg('Inside the function fun_conndb component IDF')
		fun_insert(df_4db,i_tab_nme)
	if i_action == 'Q':
		lg.echo_msg('Inside the function fun_conndb component Q')
		df=fun_query(df_4db)
		return df
	if i_action == 'T':
		lg.echo_msg('Inside the function fun_truncate component T')
		df=fun_truncate(i_tab_nme)



