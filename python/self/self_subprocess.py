import subprocess,time

def call(cmd,sleep_time=0):
    subprocess.call(cmd,shell=True)
    time.sleep(sleep_time)

