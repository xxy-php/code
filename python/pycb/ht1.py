import httplib

name_host="192.168.0.112:50071"
data_host="c05.cloudiya.com:50073"

#print "===== step1"
#conn=httplib.HTTPConnection(name_host)
#conn.request("PUT","/webhdfs/v1/levi/7?op=CREATE")
#res=conn.getresponse()
#print res.status,res.reason
#print res.msg
#conn.close()

print "===== 100 continue"
conn=httplib.HTTPConnection(data_host)
conn.connect()
#conn.putrequest("PUT","/webhdfs/v1/levi/6?op=CREATE&user.name=cloudiyadatauser")
conn.putrequest("PUT","/webhdfs/v1/levi/7?op=CREATE&overwrite=false")
conn.putheader("Content-Length",18)
conn.endheaders()
a_file=open("/home/levi/6","rb")
data=a_file.read()
a_file.close()
print data
conn.send(data)
res=conn.getresponse()
print res.status,res.reason
print res.read()
print res.msg
conn.close()