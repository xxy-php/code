from s3cmd_put import s3cmd_put_same_size
import time,sqlite3
from self.fs_analysis import file_count
from constant import sql_location

start_analysis=False

def test_cumulus_concurrency(num,base_name,bucket):
    s3cmd_put_same_size(num,base_name,bucket)
      
def log_analysis(num,sleep_time,log_location):
    result=[]
    while not start_analysis:
        time.sleep(sleep_time)
        file_num=file_count(log_location)
        if file_num==num:
            start_analysis=True
            
    for i in range(num):
        a_file=open(log_location+i+'.log','r')
        file_list=a_file.readlines()
        for j in file_list:
            if file_list.find("stored as")!=-1:
                result[i][0]=i+1
                tmp=file_list[j].split()
                result[i][1]=tmp[9]
                if tmp[12]=='KB/s':
                    result[i][2]=tmp[11]
                elif tmp[12]=='MB/s':
                    result[i][2]=tmp[11]*1024
                elif tmp[12]=='B/s':
                    result[i][2]=tmp[11]/1024
    
    conn=sqlite3.connect(sql_location+'cumulus')
    cur=conn.cursor()
    cur.executemany("insert into concurrency values (?,?,?)",result)

#s3cmd_put_same_size(1,'1M','LEVI')
#test_cumulus_concurrency(1,'1M','LEVI')