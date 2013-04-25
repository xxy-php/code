import subprocess,time

def call(cmd,sleep_time=0):
    subprocess.call(cmd,shell=True)
    time.sleep(sleep_time)
    
location="/home/levi/python_data/created_files/"

def s3cmd_put_same_size(num,base_name,bucket):
    time.sleep(10)
    for i in range(num):
        call("s3cmd put %s%s_%s s3://%s &"%(location,base_name,(i+1),bucket))
        
s3cmd_put_same_size()