import httplib

name_host="192.168.0.112:50071"
data_host="c05.cloudiya.com:50073"

print "===== step1"
conn=httplib.HTTPConnection(name_host)
conn.request("PUT","/webhdfs/v1/levi/8?op=CREATE")
res=conn.getresponse()
print res.status,res.reason
location=res.getheader("Location")
print location
index=location.find("/",7)
redirect_host=location[7:index]
url=location[index:]
print redirect_host
print url
#print res.msg
conn.close()

print "===== 100 continue"
conn=httplib.HTTPConnection(redirect_host)
conn.connect()
#conn.putrequest("PUT","/webhdfs/v1/levi/8?op=CREATE&user.name=cloudiyadatauser")
conn.putrequest("PUT",url)
conn.putheader("Content-Length",18)
conn.endheaders()
a_file=open("/home/levi/8","rb")
data=a_file.read()
a_file.close()
print data
conn.send(data)
res=conn.getresponse()
print res.status,res.reason
print res.read()
print res.msg
conn.close()