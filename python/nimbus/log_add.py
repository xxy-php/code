py_name=raw_input("python module name:")
print "Add log in %s\n"%py_name

py_file=open('%s'%py_name,'r')
lines=py_file.readlines()
new_lines=[]

for line in lines:
    def_index=line.find('def ')
    name_end_index=line.find('(')
    if def_index!=-1 and line[(def_index+4):(def_index+12)]!='__init__' and line.find(':')!=-1 and line.find('#')==-1:
        def_name=line[(def_index+4):name_end_index]
        new_line=line+(def_index+4)*' '+'pycb.log(logging.INFO, "===== def %s of %s")'%(def_name,py_name)+'\n'
        new_lines.append(new_line)
    else:
        new_line=line
        new_lines.append(new_line)
        
py_file.close()

py_file=open('%s'%py_name,"w")
py_file.writelines(new_lines)
py_file.close()