import base64
import hmac
import boto
import os
import boto.utils
import traceback
import sys
import pycb
import logging
import errno
from pycb.cbException import cbException
import time
import stat
import urllib
import glob
from pycb.cbObject import cbObject
import pynimbusauthz
from pynimbusauthz.user import User
from pynimbusauthz.user import UserAlias
from pynimbusauthz.objects import File
from pynimbusauthz.objects import UserFile
from pynimbusauthz.db import DB
import itertools

authed_user = None
public_user = None

def merge_permissions(p1, p2):
    pycb.log(logging.INFO, "===== def merge_permissions of cbAuthzSecurity.py")
    perms = p2
    for i in range(0, len(p1)):
        ndx = p2.find(p1[i])
        if ndx < 0:
            perms = perms + p1[i]
    return perms

class cbAuthzUser(object):

    def __init__(self, alias_name, con_str):
        self.db_obj = DB(con_str=con_str)
        alias = User.find_alias(self.db_obj, alias_name, pynimbusauthz.alias_type_s3)
        a_list = list(alias)
        if len(a_list) < 1:
            raise cbException('AccessDenied')
        # pick the first one, hmmm XXX
        self.alias = a_list[0]
        self.user = self.alias.get_canonical_user()

    def get_canonical_id(self):
        pycb.log(logging.INFO, "===== def get_canonical_id of cbAuthzSecurity.py")
        return self.user.get_id()

    def get_password(self):
        pycb.log(logging.INFO, "===== def get_password of cbAuthzSecurity.py")
        return self.alias.get_data()

    # return string user_id
    def get_id(self):
        pycb.log(logging.INFO, "===== def get_id of cbAuthzSecurity.py")
        return self.alias.get_name()

    # return string email name
    def get_display_name(self):
        pycb.log(logging.INFO, "===== def get_display_name of cbAuthzSecurity.py")
        return self.alias.get_friendly_name()

    def get_file_obj(self, bucketName, objectName=None):
        pycb.log(logging.INFO, "===== def get_file_obj of cbAuthzSecurity.py")
        file = File.get_file(self.db_obj, bucketName, pynimbusauthz.object_type_s3)
        pycb.log(logging.INFO, "=====## file is %s"%file)
        if file == None:
            return None
        if objectName != None:
            file = File.get_file(self.db_obj, objectName, pynimbusauthz.object_type_s3, file)
            pycb.log(logging.INFO, "=====## file is %s"%file)
        return file


    # return the permission string of the given object
    def get_uf(self, bucketName, objectName=None):
        pycb.log(logging.INFO, "===== def get_uf of cbAuthzSecurity.py")
        file = self.get_file_obj(bucketName, objectName)
        if file == None:
            pycb.log(logging.INFO, "b:o not found %s:%s" % (bucketName, str(objectName)))
            raise cbException('NoSuchKey')
        uf = UserFile(file, self.user)
        return uf

    def set_quota(self, max):
        pycb.log(logging.INFO, "===== def set_quota of cbAuthzSecurity.py")
        self.user.set_quota(max)
        self.db_obj.commit()

    def get_quota(self):
        pycb.log(logging.INFO, "===== def get_quota of cbAuthzSecurity.py")
        q = self.user.get_quota()
        self.db_obj.commit()
        return q

    # return the permission string of the given object
    def get_perms(self, bucketName, objectName=None):
        pycb.log(logging.INFO, "===== def get_perms of cbAuthzSecurity.py")
        global authed_user
        global public_user
        try:
            ufa = authed_user.get_uf(bucketName, objectName)
            ufp = public_user.get_uf(bucketName, objectName)
            p1 = ufa.get_perms(force=True)
            p2 = ufp.get_perms(force=True)
            gperms = merge_permissions(p1, p2)
        except:
            pycb.log(logging.ERROR, "error getting global permissions %s" % (sys.exc_info()[0]), tb=traceback)
            gperms = ""

        try:
            uf = self.get_uf(bucketName, objectName)
            p = uf.get_perms(force=True)
            p = merge_permissions(p, gperms)
            return (p, uf.get_file().get_data_key())
        finally:
            self.db_obj.commit()

    def get_owner(self, bucketName, objectName=None):
        pycb.log(logging.INFO, "===== def get_owner of cbAuthzSecurity.py")
        try:
            uf = self.get_uf(bucketName, objectName)
            o = uf.get_owner()
            uas = list(o.get_alias_by_type(pynimbusauthz.alias_type_s3))
            if len(uas) < 1:
                raise cbException('InternalError')
            return (uas[0].get_name(), uas[0].get_friendly_name())
        finally:
            self.db_obj.commit()

    # get a list of all of this users buckets
    # returns a list of cbObjects
    def get_my_buckets(self):
        pycb.log(logging.INFO, "===== def get_my_buckets of cbAuthzSecurity.py")
        try:
            file_iterater = File.get_user_files(self.db_obj, self.user, root=True)
            new_it = itertools.imap(lambda r: _convert_bucket_to_cbObject(self, r), file_iterater)
            return list(new_it)
        finally:
            self.db_obj.commit()

    # returns a list of cbObjects
    def list_bucket(self, bucketName, args):
        pycb.log(logging.INFO, "===== def list_bucket of cbAuthzSecurity.py")

        clause = " ORDER BY name"
        prefix = None
        if 'prefix' in args:
            prefix = args['prefix'][0]
            prefix = "%s%%" % (prefix)

        limit = None
        if 'max-keys' in args:
            max_a = args['max-keys']
            limit = int(max_a[0])

        if 'delimiter' in args:
            pass
        if 'key-marker' in args:
            km = args['key-marker'][0]
            clause = " and name > '%s'" % (km)

        try:
            bucket = File.get_file(self.db_obj, bucketName, pynimbusauthz.alias_type_s3)
            iter = bucket.get_all_children(limit=limit, match_str=prefix, clause=clause)
            new_it = itertools.imap(lambda r: _convert_File_to_cbObject(self, r), iter)
            return list(new_it)
        finally:
            self.db_obj.commit()

    # check if the given bucket/object exists
    # returns a bool 
    def exists(self, bucketName, objectName=None):
        pycb.log(logging.INFO, "===== def exists of cbAuthzSecurity.py")
        try:
            file = self.get_file_obj(bucketName, objectName)
            return file != None
        finally:
            self.db_obj.commit()

    def get_info(self, bucketName, objectName=None):
        pycb.log(logging.INFO, "===== def get_info of cbAuthzSecurity.py")
        try:
            file = self.get_file_obj(bucketName, objectName)
            return (file.get_size(), file.get_creation_time(), file.get_md5sum())
        finally:
            self.db_obj.commit()

    def get_remaining_quota(self):
        pycb.log(logging.INFO, "===== def get_remaining_quota of cbAuthzSecurity.py")
        quota = self.user.get_quota()
        if quota == User.UNLIMITED:
            return User.UNLIMITED

        u = self.user.get_quota_usage()
        return quota - u

    # add a new bucket owned by this user
    def put_bucket(self, bucketName):
        pycb.log(logging.INFO, "===== def put_bucket of cbAuthzSecurity.py")
        try:
            f = File.create_file(self.db_obj, bucketName, self.user, bucketName, pynimbusauthz.alias_type_s3)
        finally:
            self.db_obj.commit()

    def put_object(self, data_obj, bucketName, objectName):
        pycb.log(logging.INFO, "===== def put_object of cbAuthzSecurity.py")
        pycb.log(logging.INFO, "=====## data_obj is %s"%data_obj)
        data_key = data_obj.get_data_key()
        md5sum = data_obj.get_md5()
        fsize = data_obj.get_size()
        try:
            # it is ok for someone to put to an existing object
            # we just need to delete the existing one
            file = self.get_file_obj(bucketName, objectName)
            if file != None:
                pycb.config.bucket.delete_object(file.get_data_key())
                file.delete()
            bf = self.get_file_obj(bucketName)
            f = File.create_file(self.db_obj, objectName, self.user, data_key, pynimbusauthz.alias_type_s3, parent=bf, size=fsize, md5sum=md5sum)

        finally:
            self.db_obj.commit()

    # grant a new user_id access to the object or bucket
    def grant(self, user_id, bucketName, objectName=None, perms="Rr"):
        pycb.log(logging.INFO, "===== def grant of cbAuthzSecurity.py")
        try:
            uf = self.get_uf(bucketName, objectName)
            new_alias_iter = User.find_alias(self.db_obj, user_id, pynimbusauthz.alias_type_s3)
            new_alias_list = list(new_alias_iter)
            new_alias = new_alias_list[0]
            new_user = new_alias.get_canonical_user()
    
            uf.chmod(perms, user=new_user)
        finally:
            self.db_obj.commit()

    # remove an object from the registry
    def delete_object(self, bucketName, objectName):
        pycb.log(logging.INFO, "===== def delete_object of cbAuthzSecurity.py")
        try:
            file = self.get_file_obj(bucketName, objectName)
            file.delete()
        finally:
            self.db_obj.commit()

    # remove a bucket from the registry
    def delete_bucket(self, bucketName):
        pycb.log(logging.INFO, "===== def delete_bucket of cbAuthzSecurity.py")
        try:
            file = self.get_file_obj(bucketName)
            kids = file.get_all_children()
            if len(list(kids)) != 0:
                raise cbException('BucketNotEmpty')
            file.delete()
        finally:
            self.db_obj.commit()

    # return the acl list of the given bucket/object
    #
    # list of        (id, display_name, perms)
    def get_acl(self, bucketName, objectName=None):
        pycb.log(logging.INFO, "===== def get_acl of cbAuthzSecurity.py")
        try:
            file = self.get_file_obj(bucketName, objectName)
            ufs = file.get_all_user_files()

            grants = []
            for uf in ufs:
                u = uf.get_user()
                perms = uf.get_perms(force=True)
                uas = u.get_alias_by_type(pynimbusauthz.alias_type_s3)
                for ua in uas:
                    user_id = ua.get_name()
                    display_name = ua.get_friendly_name()
                    line = (user_id, display_name, perms)
                    grants.append(line)
            return grants
        finally:
            self.db_obj.commit()

    def set_user_pw(self, password):
        pycb.log(logging.INFO, "===== def set_user_pw of cbAuthzSecurity.py")
        try:
            self.alias.set_data(password)
        finally:
            self.db_obj.commit()

    def remove_user(self, force=False):
        pycb.log(logging.INFO, "===== def remove_user of cbAuthzSecurity.py")
        try:
            if force:
                uc = User(self.db_obj, uu=self.get_canonical_id(), create=False)
                uc.destroy_brutally()
            else:
                self.alias.remove()
        finally:
            self.db_obj.commit()

class cbAuthzSec(object):

    def __init__(self, con_str):
        global authed_user
        global public_user

        self.con_str = con_str

        authed_user = self.get_user(pycb.authenticated_user_id)
        public_user = self.get_user(pycb.public_user_id)

    # return a user object or raise an exception if no id is found
    def get_user(self, id):
        pycb.log(logging.INFO, "===== def get_user of cbAuthzSecurity.py")
        return cbAuthzUser(id, self.con_str)

    def create_user(self, display_name, id, pw, opts):
        pycb.log(logging.INFO, "===== def create_user of cbAuthzSecurity.py")
        db_obj = DB(con_str=self.con_str)
        user = User(db_obj, friendly=display_name)
        user_alias = user.create_alias(id, "s3", display_name, alias_data=pw)
        db_obj.commit()
        db_obj.close()

    def get_user_id_by_display(self, display_name):
        pycb.log(logging.INFO, "===== def get_user_id_by_display of cbAuthzSecurity.py")
        db_obj = DB(con_str=self.con_str)
        a_it = UserAlias.find_alias_by_friendly(db_obj, display_name)
        a_list = list(a_it)
        if len(a_list) < 1:
            return None
        alias = a_list[0]
        return alias.get_name()

    def find_user_id_by_display(self, pattern):
        pycb.log(logging.INFO, "===== def find_user_id_by_display of cbAuthzSecurity.py")
        db_obj = DB(con_str=self.con_str)
        a_it = UserAlias.find_all_alias_by_friendly(db_obj, pattern)
        new_it = map(lambda r: r.get_name(), a_it)
        return new_it

    def get_db():
        pycb.log(logging.INFO, "===== def get_db of cbAuthzSecurity.py")
        db_obj = DB(con_str=self.con_str)
        return db_obj

def _convert_test_it(a):
    pycb.log(logging.INFO, "===== def _convert_test_it of cbAuthzSecurity.py")
    return a.get_name()

def _convert_bucket_to_cbObject(user, file):
    pycb.log(logging.INFO, "===== def _convert_bucket_to_cbObject of cbAuthzSecurity.py")
    tm = file.get_creation_time()
    size = -1 
    key = file.get_name()
    display_name = file.get_name()
    obj = cbObject(tm, size, key, display_name, user)
    return obj

def _convert_File_to_cbObject(user, file):
    pycb.log(logging.INFO, "===== def _convert_File_to_cbObject of cbAuthzSecurity.py")
    data_key = file.get_data_key()
    size = file.get_size()
    tm = file.get_creation_time()
    mds = file.get_md5sum()
    key = file.get_name()
    display_name = file.get_name()
    # should file meta info come from here or backend?
    obj = cbObject(tm, size, key, display_name, user, md5sum=mds)
    return obj

