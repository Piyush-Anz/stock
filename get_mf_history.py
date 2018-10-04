
#############################################
# Author: Piyush Bijwal
#############################################


import requests
import datetime
import time
import glob
import pandas as pd

def echo_msg(t_str):
		print(datetime.datetime.now().strftime("%Y%m%d %H%M%S")+'::: '+t_str)

def fun_url_process(i_mf_cnt,i_tp_cnt,start_dt,end_dt):
	url='http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf='+str(i_mf_cnt)+'&tp='+str(i_tp_cnt)+'&frmdt='+start_dt+'&todt='+end_dt
	echo_msg('Inside the loop'+str(i_mf_cnt)+str(i_tp_cnt))
	echo_msg('URL:::'+url)
	r = requests.get(url, allow_redirects=True)
	echo_msg(datetime.datetime.now().strftime("%Y%m%d %H%M%S")+'::: After request call')
	f_filename=str(i_mf_cnt)+'_'+str(i_tp_cnt)+'.txt'
	if (r.content.find('Scheme Code') == 0):
		echo_msg('Inside if statement')
		open(f_filename, 'wb').write(r.content)

def fun_process_file(f_file):
	#f_filename='9_1.txt'
	f=open(f_file,'r')
	#f.read()
	t_title_list=[]
#	echo_msg('File successfully opened')
	title_list=f.readline().split(';')
	title_list = [x.replace(' ','') for x in title_list]
	fin_list=[]
	for line in f.read().splitlines():
		t_list= line.split(';')
		fin_list.append(t_list)
#	echo_msg('End of for loop')
	df = pd.DataFrame(fin_list,columns=title_list)
#Start: Logic to extract fund house name
	t_lst=pd.DataFrame(df['SchemeCode'].unique().tolist())
	t_mf_house=t_lst[t_lst[0].str.contains('Mutual Fund')].iloc[0][0]
	df['FundHouse']=t_mf_house
	echo_msg(f_file+'-'+t_mf_house)
#End: Logic to extract fund house name
#Start: Logic to map Scheme type and scheme code
# Capture the derived data in dataframe and load it into seperate table/file
	t_scheme_name=''
	for index, row in t_lst.iterrows():
		t_val=row.iloc[0]
		if 'Mutual Fund' not in t_val:
			if t_val.isdigit():
				t_str = t_scheme_name+';'+str(t_val)
				print t_str
			else:
				t_scheme_name=t_val


#	df = df[~df['Scheme Code'].isnull()] 

start_dt = (datetime.datetime.now() - datetime.timedelta(days=365.2425 * 10)).strftime('%d-%b-%Y')
end_dt = datetime.datetime.now().strftime("%d-%b-%Y")


i_mf_cnt=1
while (i_mf_cnt!=100):
	i_mf_cnt=i_mf_cnt+1
#	echo_msg('Calling Open Fund Function'+str(i_mf_cnt))
##Open fund
	fun_url_process(i_mf_cnt,1,start_dt,end_dt)
	time.sleep(20)

###### Below code in progress. It would help to process the file
#os.chdir("/mydir")
for f_filename in glob.glob("9_*.txt"):
#    echo_msg(f_filename)
    fun_process_file(f_filename)


