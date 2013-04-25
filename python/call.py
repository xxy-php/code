from nimbus.constant import created_files_location as location
from self.self_subprocess import call
from self.fs_analysis import file_count
#from subprocess import Popen,PIPE

import time

def s3cmd_put_same_size(num,base_name,bucket):
    for i in range(num):
        call("s3cmd put %s%s_%s s3://%s &"%(location,base_name,(i+1),bucket))
#    for i in range(2):
#        call("echo 'All %s files are uploaded.' &"%num)
    
#s3cmd_put_same_size(1,'20M','LEVI')

files_num=file_count(location)
print files_num
