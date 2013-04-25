import os
import sys
from pycb.cbException import cbException
import pycb
import stat
import urllib
import glob
import errno
import logging
import threading
import tempfile
import hashlib
import traceback
import time

class cbBackendInterface(object):

    # create and return a DataObject for writing
    # bucketName and objectName will likely just be used for seeding
    # internal names, the plug should not need these values
    def put_object(self, bucketName, objectName):
        pycb.log(logging.INFO, "===== def put_object of cbBackendInterface.py")
        return obj


    # copy an object, not yet implemented
    def copy_object(self, srcObjectName, dstObjectName, moveOrCopy, httpHeaders):
        pycb.log(logging.INFO, "===== def copy_object of cbBackendInterface.py")
        pass


    # find and return a dataobject with the given key for reading
    def get_object(self, data_key):
        pycb.log(logging.INFO, "===== def get_object of cbBackendInterface.py")
        obj = cbPosixData(data_key, "r")
        return obj


    # delete the data in the given datakey
    def delete_object(self, data_key):
        pycb.log(logging.INFO, "===== def delete_object of cbBackendInterface.py")
        pass

    # return the size of the dataset associated with the given key
    def get_size(self, data_key):
        pycb.log(logging.INFO, "===== def get_size of cbBackendInterface.py")
        pass

    # get the modification time
    def get_mod_time(self, data_key):
        pycb.log(logging.INFO, "===== def get_mod_time of cbBackendInterface.py")
        pass

    # get the md5 sum of the file
    def get_md5(self, data_key):
        pycb.log(logging.INFO, "===== def get_md5 of cbBackendInterface.py")
        pass

class cbDataObject(object):

    def get_data_key(self):
        pycb.log(logging.INFO, "===== def get_data_key of cbBackendInterface.py")
        pass

    # this is part of the work around for twisted
    def set_delete_on_close(self, delete_on_close):
        pycb.log(logging.INFO, "===== def set_delete_on_close of cbBackendInterface.py")
        pass

    # implement file-like methods
    def close(self):
        pycb.log(logging.INFO, "===== def close of cbBackendInterface.py")
        pass

    def flush(self):
        pycb.log(logging.INFO, "===== def flush of cbBackendInterface.py")
        pass

    #def fileno(self):
    #def isatty(self):

    def next(self):
        pycb.log(logging.INFO, "===== def next of cbBackendInterface.py")
        pass

    def read(self, size=None):
        pycb.log(logging.INFO, "===== def read of cbBackendInterface.py")
        pass

#    def readline(self, size=None):
#    def readlines(self, size=None):
#    def xreadlines(self):

    def seek(self, offset, whence=None):
        pycb.log(logging.INFO, "===== def seek of cbBackendInterface.py")
        pass

#    def tell(self):
#    def truncate(self, size=None):

    def write(self, st):
        pycb.log(logging.INFO, "===== def write of cbBackendInterface.py")
        pass

    def writelines(self, seq):
        pycb.log(logging.INFO, "===== def writelines of cbBackendInterface.py")
        pass

