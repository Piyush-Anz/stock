#############################################
# Author: Piyush Bijwal
#############################################


import requests
import datetime
import time
import glob
import pandas as pd
from sqlalchemy import create_engine as ce
import os
import psycopg2

def postgres_test():
    try:
        conn = psycopg2.connect("dbname='piyushbijwal' user='piyushbijwal' host='localhost' password=os.getenv('logpwd') connect_timeout=1 ")
        conn.close()
        return True
    except:
        return False

def echo_msg(t_str):
		print(datetime.datetime.now().strftime("%Y%m%d %H%M%S")+'::: '+t_str)

def fun_url_process(i_mf_cnt,i_tp_cnt,start_dt,end_dt,a_filealias):
	url='http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf='+str(i_mf_cnt)+'&tp='+str(i_tp_cnt)+'&frmdt='+start_dt+'&todt='+end_dt
	f_filename=str(i_mf_cnt)+'_'+str(i_tp_cnt)+a_filealias
	echo_msg('Inside the function'+str(i_mf_cnt)+str(i_tp_cnt))
	echo_msg('URL:::'+url)
	if os.path.isfile(f_filename):
		echo_msg('File already exists: '+f_filename)
		return
	r = requests.get(url, allow_redirects=True)
	echo_msg(datetime.datetime.now().strftime("%Y%m%d %H%M%S")+'::: After request call')
	if (r.content.find('Scheme Code') == 0):
		echo_msg('Inside Scheme Code validation statement')
		open(f_filename, 'wb').write(r.content)

# url='https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=NAB.AX&apikey=220YSYTZLTKHFPQQ&datatype=csv'

def fun_conndb(df_4db,i_tab_nme, i_action):
	dbengine = ce('postgresql://piyushbijwal:os.getenv("logpwd")@localhost:5432/piyushbijwal')
	if i_action == 'I':
		df_4db.head(0).to_sql(i_tab_nme, con=dbengine,if_exists='append')
		df_4db.to_sql(i_tab_nme, con=dbengine,if_exists='append')

def fun_process_file(f_file):
	#f_file='9_1.txt'
	f=open(f_file,'r')
	#f.read()
	t_title_list=[]
#	echo_msg('File successfully opened')
	l_title_list=f.readline().split(';')
	l_title_list = [x.replace(' ','') for x in l_title_list]
	l_fin_list=[]
	for line in f.read().splitlines():
		t_list= line.split(';')
		l_fin_list.append(t_list)
#	echo_msg('End of for loop')
	df = pd.DataFrame(l_fin_list,columns=l_title_list)
#Start: Logic to extract fund house name
	t_lst=pd.DataFrame(df['SchemeCode'].unique().tolist())
	t_mf_house=t_lst[t_lst[0].str.contains('Mutual Fund')].iloc[0][0]
	df['FundHouse']=t_mf_house
	df = df.loc[df.SchemeCode.str.isdigit()]
	fun_conndb(df,'t_mf_txn','I')
	echo_msg(f_file+'-'+t_mf_house)
#End: Logic to extract fund house name
#Start: Logic to map Scheme type and scheme code
	t_scheme_name=''
	l_mf_schtyp=[]
	l_title_list=['FundHouse','SchemeType','SchemeCode']
	df_mf_sch_typ = pd.DataFrame(columns=l_title_list)
	for index, row in t_lst.iterrows():
		t_val=row.iloc[0]
		if 'Mutual Fund' not in t_val:
			if t_val.isdigit():
				t_list=[t_mf_house,t_scheme_name,str(t_val)]
				l_mf_schtyp.append(t_list)
#				t_str=t_mf_house+";"+t_scheme_name+";"+str(t_val)
#				print l_mf_schtyp
			else:
				t_scheme_name=t_val
	df_mf_sch_typ = pd.DataFrame(l_mf_schtyp,columns=l_title_list)
	fun_conndb(df_mf_sch_typ,'t_mf_sch_type','I')
#	print df_mf_sch_typ
#End: Logic to map Scheme type and scheme code


start_dt = (datetime.datetime.now() - datetime.timedelta(days=365.2425 * 10)).strftime('%d-%b-%Y')
end_dt = datetime.datetime.now().strftime("%d-%b-%Y")
a_filealias = "_"+datetime.datetime.now().strftime("%Y%m%d")+'mflst.txt'
p_filepath='/Users/piyushbijwal/Documents/Project/stock_analysis/MyScripts/DataSet/'
p_curr_path=os.getcwd()
os.chdir(p_filepath)

i_mf_cnt=1
while (i_mf_cnt!=3):
	i_mf_cnt=i_mf_cnt+1
#	echo_msg('Calling Open Fund Function'+str(i_mf_cnt))
##Open fund
	fun_url_process(i_mf_cnt,1,start_dt,end_dt,a_filealias)
	time.sleep(20)

echo_msg('Dataset creation completed')
echo_msg('-----------------------------------------------------')
###### Below code in progress. It would help to process the file
echo_msg('File processing activity started')
for f_filename in glob.glob('*'+a_filealias):
    echo_msg(f_filename)
    fun_process_file(f_filename)

os.chdir(p_curr_path)

echo_msg('-----------------------------------------------------')
