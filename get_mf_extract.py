#############################################
# Author: Piyush Bijwal
# Comment: This service would be responsible to download the dataset from external providers.
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

def fun_url_process(i_mf_cnt,i_tp_cnt,start_dt,end_dt,a_filealias):
	url='http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf='+str(i_mf_cnt)+'&tp='+str(i_tp_cnt)+'&frmdt='+start_dt+'&todt='+end_dt
	f_filename=str(i_mf_cnt)+'_'+str(i_tp_cnt)+a_filealias
	lg.echo_msg('Inside the function'+str(i_mf_cnt)+str(i_tp_cnt))
	lg.echo_msg('URL:::'+url)
	if os.path.isfile(f_filename):
		lg.echo_msg('File already exists: '+f_filename)
		return
	r = requests.get(url, allow_redirects=True)
	lg.echo_msg(datetime.datetime.now().strftime("%Y%m%d %H%M%S")+'::: After request call')
	if (r.content.find('Scheme Code') == 0):
		lg.echo_msg('Inside Scheme Code validation statement')
		open(f_filename, 'wb').write(r.content)

def fun_mf_extract(i_mf_cnt,i_tp_cnt,start_dt,end_dt,f_filename):
	url='http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf='+str(i_mf_cnt)+'&tp='+str(i_tp_cnt)+'&frmdt='+start_dt+'&todt='+end_dt
	lg.echo_msg('Inside the function'+str(i_mf_cnt)+str(i_tp_cnt))
	lg.echo_msg('URL:::'+url)
	if os.path.isfile(f_filename):
		lg.echo_msg('File already exists: '+f_filename)
		return
	r = requests.get(url, allow_redirects=True)
	lg.echo_msg(datetime.datetime.now().strftime("%Y%m%d %H%M%S")+'::: After request call')
	if (r.content.find('Scheme Code') == 0):
		lg.echo_msg('Inside Scheme Code validation statement')
		open(f_filename, 'wb').write(r.content)


start_dt = (datetime.datetime.now() - datetime.timedelta(days=365.2425 * 10)).strftime('%d-%b-%Y')
end_dt = datetime.datetime.now().strftime("%d-%b-%Y")
a_filealias = "_"+datetime.datetime.now().strftime("%Y%m%d")+'mflst.txt'
p_filepath=os.getenv("MFDATASET")
p_curr_path=os.getcwd()
os.chdir(p_filepath)

l_qry_str="select filename, date_loaded,download_status from t_file_control where download_status='SUCCESS'"
df_avail_data=pd.DataFrame(columns=['filename', 'date_loaded','download_status'])
df_avail_data=db.fun_execreq(l_qry_str,'', 'Q')

##Open fund
i_mf_cnt=1
i_tp_cnt=1	##Indicate Open Ended fund
l_fin_list=[]
while (i_mf_cnt!=3):
	i_mf_cnt=i_mf_cnt+1
#	echo_msg('Calling Open Fund Function'+str(i_mf_cnt))
	f_filename=str(i_mf_cnt)+'_'+str(i_tp_cnt)+a_filealias
	l_fin_list.append(f_filename)

df_mf_fresh_data=pd.DataFrame(l_fin_list,columns=["filename"])

#np.where(df_avail_data['filename']!=df_mf_fresh_data['filename'])
if df_avail_data.empty:
	for index, row in df_mf_fresh_data.iterrows():
		try:
			fun_mf_extract(i_mf_cnt,i_tp_cnt,start_dt,end_dt,row['filename'])
			time.sleep(20)
		except:
			lg.echo_msg('Fail to extract data for '+filename)

#	fun_url_process(i_mf_cnt,1,start_dt,end_dt,a_filealias)

lg.echo_msg('Dataset creation completed')
os.chdir(p_curr_path)
lg.echo_msg('-----------------------------------------------------')


