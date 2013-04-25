import subprocess,time,os,sqlite3
from datetime import datetime
from datetime import timedelta

start_analysis=False

def call(cmd,sleep_time=0):
    subprocess.call(cmd,shell=True)
    time.sleep(sleep_time)
    
location="./files/"

def create_1mB():
    a_str=''
    for i in range(1024*1024*8):
        a_str+='0'
    return a_str

def create_same_size_mB(base_name,size=1,num=1):
    mB=create_1mB()
    for i in range(num):
        print "File %s_%s is being created."%(base_name,(i+1))
        a_file=open("%s%s_%s"%(location,base_name,(i+1)),'w')
        for j in range(size):
            a_file.writelines(mB)
        a_file.close()
    print "All files are created."
    

def s3cmd_put_same_size(num,base_name,bucket):
    log_location = './log/'+str(num)+'/'
    os.mkdir(log_location)
    for i in range(num):
        call("s3cmd put %s%s_%s s3://%s > %s%s.log &"%(location,base_name,(i+1),bucket,log_location,(i+1)))
        
def file_count(dirname,filter_types=[]):
    '''Count the files in a directory includes its subfolder's files
       You can set the filter types to count specific types of file'''
    count=0
    filter_is_on=False
    if filter_types!=[]: filter_is_on=True
    for item in os.listdir(dirname):
        abs_item=os.path.join(dirname,item)
        #print item
        if os.path.isdir(abs_item):
            #Iteration for dir
            count+=file_count(abs_item,filter_types)
        elif os.path.isfile(abs_item):
            if filter_is_on:
                #Get file's extension name
                extname=os.path.splitext(abs_item)[1]
                if extname in filter_types:
                    count+=1
            else:
                count+=1
    return count
        
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
    
    conn=sqlite3.connect('./cumulus')
    cur=conn.cursor()
    cur.executemany("insert into concurrency values (?,?,?)",result)
        
create_same_size_mB('2M',2,1000)

#9
time.sleep(9000)
s3cmd_put_same_size(500,'2M','LEVI')
log_analysis(500,3600,'./log/')


