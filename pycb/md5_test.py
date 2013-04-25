import hashlib,os

md5=hashlib.md5()
file_path="/home/levi/50k"
a_file=open(file_path,"rb")
status=os.stat(file_path)
length=status.st_size
remain=length
j=10
chunk=length/j
for i in range(j):
    if remain>chunk:
        arg=a_file.read(chunk)
        md5.update(arg)
    else:
        arg=a_file.read(remain)
        md5.update(arg)
    remain-=chunk
hash=md5.hexdigest()
print hash

#md5=hashlib.md5()
#file_path="/home/levi/50k"
#a_file=open(file_path,"rb")
#arg=a_file.read()
#md5.update(arg)
#hash=md5.hexdigest()
#print hash