from constant import created_files_location,python_data_location
from self.self_subprocess import call
import time
import os
#def s3cmd_put_same_size(num,base_name,bucket):
#    start_time=time.time()
#    start_time_format=time.strftime("%H%M%S")
#    print "putting start at %s"%start_time_format
#    for i in range(num):
#        sp("s3cmd put %s%s_%s s3://%s &"%(location,base_name,(i+1),bucket))
#        
#    print ""
#    print "All %s files are uploaded."%num

def s3cmd_put_same_size(num,base_name,bucket):
    log_location = python_data_location + 'log/'+str(num)+'/'
    os.mkdir(log_location)
    for i in range(num):
        call("s3cmd put %s%s_%s s3://%s > %s%s.log &"%(created_files_location,base_name,(i+1),bucket,log_location,(i+1)))