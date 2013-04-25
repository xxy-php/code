from self_subprocess import call
from constant import created_files_location as location

def create_1kB():
    a_str=''
    for i in range(1024*8):
        a_str+='0'
    return a_str

def create_1mB():
    a_str=''
    for i in range(1024*1024*8):
        a_str+='0'
    return a_str

def get_info(num):
    name=[]
    size=[]
    if num==1:
        name.append(raw_input('file name:'))
        size.append(int(raw_input('file size(KB):')))
    else:
        for i in range(num):
            name.append(raw_input('file name:'))
            size.append(int(raw_input('file size(KB):')))
    return name,size

def create_file(num=1):
    name,size=get_info(num)
    kB=create_1kB()
    for i in range(num):
        print "File "+name[i]+" is being created."
        a_file=open('/home/levi/python_data/created_files/'+name[i],'w')
        for j in range(size[i]):
            a_file.writelines(kB)
        a_file.close()
    print "All files are created."
    
def create_same_size(base_name,size=1,num=1):
    kB=create_1kB()
    for i in range(num):
        print "File %s_%s is being created."%(base_name,(i+1))
        a_file=open("%s%s_%s"%(location,base_name,(i+1)),'w')
        for j in range(size):
            a_file.writelines(kB)
        a_file.close()
    print "All files are created."
    
def create_same_size_mB(base_name,size=1,num=1):
    mB=create_1mB()
    for i in range(num):
        print "File %s_%s is being created."%(base_name,(i+1))
        a_file=open("%s%s_%s"%(location,base_name,(i+1)),'w')
        for j in range(size):
            a_file.writelines(mB)
        a_file.close()
    print "All files are created."
    
def create_same_size_cp(base_name,size=1,num=1):
    kB=create_1kB()
    
    a_file=open(location+base_name+'_1','w')
    print "File %s_1 is being created."%base_name
    for i in range(size):
        a_file.writelines(kB)
    a_file.close()
    
    for i in range(num-1):
        print "File %s_%s is being created."%(base_name,(i+2))
        call("cp %s%s_1 %s%s_%s"%(location,base_name,location,base_name,(i+2)))
    print "All files are created."
