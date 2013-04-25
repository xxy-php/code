from s3cmd_put import s3cmd_put_same_size
import time,sqlite3

def test_cumulus_concurrency(num,base_name,bucket):
    s3cmd_put_same_size(num,base_name,bucket)
      
def log_analysis():
    pass

#s3cmd_put_same_size(1,'1M','LEVI')
test_cumulus_concurrency(1,'1M','LEVI')