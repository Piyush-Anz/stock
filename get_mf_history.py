
#############################################
# Author: Piyush Bijwal
#############################################


import requests
import datetime
import time

def fun_url_process(i_mf_cnt,i_tp_cnt,start_dt,end_dt):
	url='http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf='+str(i_mf_cnt)+'&tp='+str(i_tp_cnt)+'&frmdt='+start_dt+'&todt='+end_dt
	print(datetime.datetime.now().strftime("%Y%m%d%H%M%S")+'::: Inside the loop'+str(i_mf_cnt)+str(i_tp_cnt))
	print('URL:::'+url)
	r = requests.get(url, allow_redirects=True)
	print(datetime.datetime.now().strftime("%Y%m%d%H%M%S")+'::: After request call')
	f_filename=str(i_mf_cnt)+'_'+str(i_tp_cnt)+'.txt'
	if (r.content.find('Scheme Code') == 0):
		print('Inside if statement')
		open(f_filename, 'wb').write(r.content)


start_dt = (datetime.datetime.now() - datetime.timedelta(days=365.2425 * 10)).strftime('%d-%b-%Y')
end_dt = datetime.datetime.now().strftime("%d-%b-%Y")


i_mf_cnt=1
while (i_mf_cnt!=100):
	i_mf_cnt=i_mf_cnt+1
	print(datetime.datetime.now().strftime("%Y%m%d%H%M%S")+'::: Calling Open Fund Function'+str(i_mf_cnt))
##Open fund
	fun_url_process(i_mf_cnt,1,start_dt,end_dt)
	time.sleep(20)

###### Below code in progress. It would help to process the file
i_linenum=1
f_filename='9_1.txt'
f=open(f_filename,'r')
#f.read()
t_title_list=[]
fin_list=[]
for line in f.read().splitlines():
	t_list= line.split(';')
	fin_list.append(t_list)

df=pd.DataFrame(fin_list,columns=fin_list[0])
t_fund_list=[]
t_fund_list.append(df_uniq[df_uniq[0].str.contains('Mutual Fund')])
t_fund_list
df = df[~df['Scheme Code'].isnull()] 
