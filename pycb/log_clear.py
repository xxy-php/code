py_name=raw_input("python module name:")
print "clear log in %s\n"%py_name

py_file=open('%s'%py_name,'r')
lines=py_file.readlines()
new_lines=[]

for line in lines:
    if line.find('pycb.log(logging.INFO, "===== def')==-1 and line.find('pycb.log(logging.INFO, "===== class')==-1:
        new_line=line
        new_lines.append(new_line)
        
py_file.close()

py_file=open('%s'%py_name,"w")
py_file.writelines(new_lines)
py_file.close()