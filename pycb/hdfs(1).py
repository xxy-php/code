#import os
#import sys
#from pycb.cbException import cbException
#import pycb
#import errno
#import logging
#import tempfile
#import hashlib
#import traceback
#import time
#import pycb
import httplib,json

def redirect_host_url(res):
    location=res.getheader("Location")
    index=location.find("/",7)
    redirect_host=location[7:index]
    url=location[index:]
    return redirect_host,url

class HDFS(object):
    
    def __init__(self,name_node):
        self.name_node=name_node
    
    def put_object(self,bucketName,objectName,len):
        conn=httplib.HTTPConnection(self.name_node)
        conn.request("PUT","/webhdfs/v1/%s/%s?op=CREATE"%(bucketName,objectName))
        #conn.request("PUT","/webhdfs/v1/%s/%s?op=CREATE"%(bucketName,objectName))
        res=conn.getresponse()
        redirect_host,url=redirect_host_url(res)
        conn.close()
        
        conn=httplib.HTTPConnection(redirect_host)
        conn.connect()
        conn.putrequest("PUT",url)
        conn.putheader("Content-Length",len)
        conn.endheaders()
        return conn
    
    def get_object(self,bucketName,objectName):
        conn=httplib.HTTPConnection(self.name_node)
        conn.request("GET","/webhdfs/v1/%s/%s?op=OPEN"%(bucketName,objectName))
        res=conn.getresponse()
        redirect_host,url=redirect_host_url(res)
        conn.close()
        
        conn=httplib.HTTPConnection(redirect_host)
        conn.request("GET",url)
        return conn
    
    def put_bucket(self,bucketName):
        conn=httplib.HTTPConnection(self.name_node)
        conn.request("PUT","/webhdfs/v1/%s?op=MKDIRS"%bucketName)
        return conn
    
    #This method is not intended for cumulus.
    def rename_bucket(self,bucketName,destination):
        conn=httplib.HTTPConnection(self.name_node)
        conn.request("PUT","/webhdfs/v1/%s?op=RENAME&destination=/%s"%(bucketName,destination))
        return conn
    
    #This method is intended for deleting both object and bucket.
    def delete_object(self,objectName):
        conn=httplib.HTTPConnection(self.name_node)
        conn.request("DELETE","/webhdfs/v1/%s?op=DELETE"%objectName)
        return conn
    
    def get_status(self,objectName):
        conn=httplib.HTTPConnection(self.name_node)
        conn.request("GET","/webhdfs/v1/%s?op=GETFILESTATUS"%objectName)
        return conn
    
    def list_bucket(self,bucketName):
        conn=httplib.HTTPConnection(self.name_node)
        conn.request("GET","/webhdfs/v1/%s?op=LISTSTATUS"%bucketName)
        return conn
    
    def get_md5(self,bucketName,objectName):
        conn=httplib.HTTPConnection(self.name_node)
        conn.request("GET","/webhdfs/v1/%s/%s?op=GETFILECHECKSUM"%(bucketName,objectName))
        res=conn.getresponse()
        redirect_host,url=redirect_host_url(res)
        conn.close()
        
        conn=httplib.HTTPConnection(redirect_host)
        conn.request("GET",url)
        return conn
        
    
if __name__=="__main__":
    hdfs=HDFS("192.168.0.112:50071")
    
    def get_res(conn):
        res=conn.getresponse()
        status=res.status
        reason=res.reason
        msg=res.msg
        body=res.read()
        print status,reason
        print msg
        print body
        conn.close()
        return status,reason,msg,body
        
        
    #put_object
#    conn=hdfs.put_object("LEVI","50k",51200)
#    a_file=open("/home/levi/50k","rb")
#    data=a_file.read()
#    conn.send(data) #sent by chunk in cumulus.
#    res=conn.getresponse()
#    print res.status,res.reason
#    print res.msg
#    print res.read()
    
    #get_object
#    conn=hdfs.get_object("LEVI", "4")
#    get_res(conn)

    #put_bucket
#    conn=hdfs.put_bucket("LEVI1")
#    get_res(conn)

    #rename_bucket
#    conn=hdfs.rename_bucket("LEVI1", "LEVI4")
#    get_res(conn)

    #delete_object (can also delete bucket)
#    conn=hdfs.delete_object("LEVI4")
#    get_res(conn)

    #get_status
#    conn=hdfs.get_status("LEVI/5")
#    get_res(conn)

    #list_bucket
#    conn=hdfs.list_bucket("LEVI")
#    json_obj=get_res(conn)[3]
#    a_dict=json.loads(json_obj)
#    print a_dict["FileStatuses"]["FileStatus"][0]["pathSuffix"]
#    print a_dict["FileStatuses"]["FileStatus"][0]["length"]

    #get_md5
#    conn=hdfs.get_md5("LEVI","50k")
#    res=conn.getresponse()
#    json_obj=res.read()
#    print type(json_obj)
#    print json_obj
#    md5_dict=json.loads(json_obj)
#    print md5_dict
#    md5=md5_dict["FileChecksum"]["bytes"]
#    print md5
    
    
    
    
    
