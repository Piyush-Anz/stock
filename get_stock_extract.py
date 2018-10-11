#############################################
# Author: Piyush Bijwal
# Comment: This service would be responsible to download the dataset from external providers.
# fun_url_alpha- Responsible to generate the url string for "Alpha vantage" data point. It takes input for stock name like NAB.AX
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


def fun_url_alpha(i_stock):
	apikey=os.getenv('AVANTAGEKEY')
	if not apikey:
		lg.echo_msg('API Key not found. Register https://www.alphavantage.co and set env variable AVANTAGEKEY')
		return 99
	else:
		url='https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol='+str(i_stock)+'&apikey='+str(apikey)+'&datatype=csv'
		lg.echo_msg(url)
		return url

def fun_download_url(i_url):
	if not i_url:
		lg.echo_msg('i_url variable not set')
		return 99
	try:
		r_response = requests.get(i_url, allow_redirects=True)
		return r_response
	except requests.ConnectionError:
		lg.echo_msg('Unable to establish connection with url. Check url string or service')
		return 99


def fun_respone_2_df(r_response):
	l_title=(r_response.text.split("\r\n")[0].encode('ascii').split(","))
	print l_title
	l_data=[]
	for i in r_response.text.split("\r\n")[1:]:
		if i != "":
			l_data.append(i.split(","))
	df_stock=pd.DataFrame(l_data,columns=l_title)
	return df_stock



