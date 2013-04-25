import os
import sys
from pycb.cbException import cbException
import pycb
import errno
import logging
import tempfile
import hashlib
import traceback
import time
import pycb
import pycurl

class cbPosixBackend(object):

    def __init__(self, installdir):
        self.base_dir = installdir

        try:
            os.mkdir(self.base_dir)
        except:
            pass
        try:
            os.mkdir(self.base_dir + "/buckets")
        except:
            pass

    # The POST request operation adds an object to a bucket using HTML forms.
    #
    #  Not implemented for now
    def post_object(self):
        pycb.log(logging.INFO, "===== def post_object of cbPosixBackend.py")
        ex = cbException('NotImplemented')
        raise ex


    # The PUT request operation adds an object to a bucket.
    #  In this operation we return a data object for writing to the cb
    #  store.  The controlling code will handle moving buffers to it
    # 
    # returns <return code>,<error msg>,<data object | None>
    # 0 indicates success
    # 
    def put_object(self, bucketName, objectName):
        pycb.log(logging.INFO, "===== def put_object of cbPosixBackend.py")
        # first make the bucket directory if it does not exist
        dir_name = bucketName[:1]
        bdir = self.base_dir + "/" + dir_name
        
        try:
            os.mkdir(bdir)
        except OSError, ose:
            if ose.errno != 17:
                raise

        fname = bucketName + "/" + objectName
        fname = fname.replace("/", "__")
        (osf, x) = tempfile.mkstemp(dir=bdir, suffix=fname)
        os.close(osf)
        data_key = x.strip()
        obj = cbPosixData(data_key, "w+b")
        return obj


    # Return 2 data objects, a source and dest.
    #
    # returns <return code>,<error msg>,<data object|None>,<data object|None>
    #
    def copy_object(self, srcObjectName, dstObjectName, moveOrCopy, httpHeaders):
        pycb.log(logging.INFO, "===== def copy_object of cbPosixBackend.py")
        ex = cbException('NotImplemented')
        raise ex


    # returns a data object for reading.  The controlling code will handle
    # the buffer management.
    #
    # returns <return code>,<error msg>,<data object | None>
    def get_object(self, data_key):
        pycb.log(logging.INFO, "===== def get_object of cbPosixBackend.py")
        obj = cbPosixData(data_key, "r")
        return obj


    # The DELETE request operation removes the specified object from cb
    #   
    # returns <return code>,<error message | None>
    def delete_object(self, data_key):
        pycb.log(logging.INFO, "===== def delete_object of cbPosixBackend.py")
        obj = cbPosixData(data_key, openIt=False)
        obj.delete()

    def get_size(self, data_key):
        pycb.log(logging.INFO, "===== def get_size of cbPosixBackend.py")
        st = os.stat(data_key)
        return st.st_size

    def get_mod_time(self, data_key):
        pycb.log(logging.INFO, "===== def get_mod_time of cbPosixBackend.py")
        st = os.stat(data_key)
        return time.gmtime(st.st_ctime)

    def get_md5(self, data_key):
        pycb.log(logging.INFO, "===== def get_md5 of cbPosixBackend.py")
        obj = cbPosixData(data_key, "r")
        hash = obj.get_md5()
        obj.close()
        return hash

class cbPosixData(object):

    def __init__(self, data_key, access="r", openIt=True):
        self.fname = data_key
        self.metafname = data_key + ".meta"
        self.data_key = data_key
        self.blockSize = pycb.config.block_size
        self.hashValue = None
        self.delete_on_close = False
        self.md5er = hashlib.md5()
        self.access = access

        self.seek_count = 0

        if not openIt:
            return

        try:
            # this allows head to be very fast
            if access == "r":
                mFile = open(self.metafname, 'r')
                self.hashValue = mFile.readline()
                mFile.close()
        except:
            pass

        try:
            self.file = open(self.fname, access)
        except OSError, (OsEx):
            if OsEx.errno == errno.ENOENT:
                raise cbException('NoSuchKey')

    def get_mod_time(self):
        pycb.log(logging.INFO, "===== def get_mod_time of cbPosixBackend.py")
        st = os.stat(self.data_key)
        return time.gmtime(st.st_ctime)

    def get_md5(self):
        pycb.log(logging.INFO, "===== def get_md5 of cbPosixBackend.py")
        if self.hashValue == None:
            v = str(self.md5er.hexdigest()).strip()
            return v
        pycb.log(logging.INFO, "=====## self.hashValue is %s"%self.hashValue)
        return self.hashValue

    def get_data_key(self):
        pycb.log(logging.INFO, "===== def get_data_key of cbPosixBackend.py")
        return self.data_key

    def get_size(self):
        pycb.log(logging.INFO, "===== def get_size of cbPosixBackend.py")
        return os.path.getsize(self.fname)

    def delete(self):
        pycb.log(logging.INFO, "===== def delete of cbPosixBackend.py")
        try:
            os.unlink(self.metafname)
        except Exception, ex:
            pycb.log(logging.WARNING, "error deleting %s %s %s" % (self.metafname, str(sys.exc_info()[0]), str(ex)))
        try:
            os.unlink(self.data_key)
        except:
            pycb.log(logging.WARNING, "error deleting %s %s" % (self.data_key, str(sys.exc_info()[0])))

    def set_md5(self, hash):
        pycb.log(logging.INFO, "===== def set_md5 of cbPosixBackend.py")
        self.hashValue = hash

    # this is part of the work around for twisted
    def set_delete_on_close(self, delete_on_close):
        pycb.log(logging.INFO, "===== def set_delete_on_close of cbPosixBackend.py")
        self.delete_on_close = delete_on_close

    # implement file-like methods
    def close(self):
        pycb.log(logging.INFO, "===== def close of cbPosixBackend.py")
        hashValue = self.get_md5()
        if hashValue != None:
            try:
                mFile = open(self.metafname, 'w')
                mFile.write(hashValue)
                mFile.close()
            except:
                pass

        try:
            self.file.close()
        except Exception, ex:
            pycb.log(logging.WARNING, "error closing data object %s %s" % (self.fname, sys.exc_info()[0]), tb=traceback)
        if self.delete_on_close:
            pycb.log(logging.INFO, "deleting the file on close %s" % (self.fname))
            self.delete()


    def flush(self):
        pycb.log(logging.INFO, "===== def flush of cbPosixBackend.py")
        return self.file.flush()

    # def fileno(self):
    # def isatty(self):

    def next(self):
        pycb.log(logging.INFO, "===== def next of cbPosixBackend.py")
        return self.file.next()

    def read(self, size=None):
        pycb.log(logging.INFO, "===== def read of cbPosixBackend.py")
        if size == None:
            st = self.file.read(self.blockSize)
        else:
            st = self.file.read(size)
        self.md5er.update(st)
        return st

#    def readline(self, size=None):
#    def readlines(self, size=None):
#    def xreadlines(self):

    def seek(self, offset, whence=None):
        pycb.log(logging.INFO, "===== def seek of cbPosixBackend.py")
        self.seek_count = self.seek_count + 1
        pycb.log(logging.WARNING, "Someone is seeking %s %d :: %d" % (self.fname, offset, self.seek_count), tb=traceback)
        if self.seek_count > 1:
            raise cbException('InternalError')
        return self.file.seek(offset, whence)


#    def tell(self):
#    def truncate(self, size=None):

    def write(self, st):
        pycb.log(logging.INFO, "===== def write of cbPosixBackend.py")
        self.file.write(st)
        self.md5er.update(st)
        
#    def write(self, st):
#        pycb.log(logging.INFO, "===== def write of cbPosixBackend.py")
#        c=pycurl.Curl()
#        c.setopt(pycurl.URL,"http://localhost:50075/webhdfs/v1/levi/7?op=CREATE&user.name=levi")
#        c.setopt(pycurl.CUSTOMREQUEST,"PUT")
#        c.setopt(pycurl.HTTPPOST,[("textname",(c.FORM_FILE,st))])
#        c.perform()
#        c.close()

    def writelines(self, seq):
        pycb.log(logging.INFO, "===== def writelines of cbPosixBackend.py")
        for s in seq:
            self.write(s)
        


