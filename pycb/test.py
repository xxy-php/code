import os

status=os.stat("/home/levi/50k")
print status
print status.st_size