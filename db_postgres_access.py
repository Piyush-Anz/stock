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
        conn = psycopg2.connect("dbname='piyushbijwal' user='piyushbijwal' host='localhost' password=os.getenv('logpwd') connect_timeout=1 ")
        lg.echo_msg('DB Connection test successfull')
#        conn.close()
        return conn
    except:
    	lg.echo_msg('DB Connection test unsuccessfull')
        return False

def fun_execreq(df_4db,i_tab_nme, i_action):
	### Make the below string as an environment variable
	conn=postgres_conn()
	if i_action == 'IDF':
		lg.echo_msg('Inside the function fun_conndb component IDF')
		try:
			dbengine = ce('postgresql://piyushbijwal:os.getenv("logpwd")@localhost:5432/piyushbijwal')
			df_4db.head(0).to_sql(i_tab_nme, con=dbengine,if_exists='append')
			df_4db.to_sql(i_tab_nme, con=dbengine,if_exists='append')
			conn.close()
			return True
		except:
			lg.echo_msg('Error in function fun_conndb component IDF')
			return False
	if i_action == 'Q':
		lg.echo_msg('Inside the function fun_conndb component Q')
		try:
			cur = conn.cursor()
			cur.execute(df_4db)
			fetch_rec = cur.fetchall()
			df= pd.DataFrame(list(fetch_rec))
			return df
		except:
			lg.echo_msg('Error in function fun_conndb component Q')
			return False




