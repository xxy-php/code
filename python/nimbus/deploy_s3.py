import subprocess

def call(cmd):
    subprocess.call(cmd,shell=True)
    
call("echo '123'|sudo -S cp /home/levi/Dropbox/workspace/pycb/S3.py /usr/lib/python2.6/site-packages/S3")