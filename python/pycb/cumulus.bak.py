#!/usr/bin/env python

from twisted.web import server, resource, http
from twisted.internet import reactor, ssl
from cbPosixBackend import cbPosixBackend
from ConfigParser import SafeConfigParser
import hashlib
from pycb.cbException import cbException
from pycb.cbRequest import cbGetService
from pycb.cbRequest import cbGetBucket
from pycb.cbRequest import cbGetObject
from pycb.cbRequest import cbDeleteBucket
from pycb.cbRequest import cbDeleteObject
from pycb.cbRequest import cbPutBucket
from pycb.cbRequest import cbPutObject
from pycb.cbRequest import cbHeadObject
from pycb.cbRequest import cbCopyObject
from pycb.cbRedirector import *
from datetime import date, datetime
from xml.dom.minidom import Document
import uuid
import urllib
import traceback
import sys
import os
import socket
import logging
import pycb
import threading
import tempfile
import threading
import hdfs

def end_redirector(result, request):
    pycb.log(logging.INFO, "===== def end_redirector of cumulus.py")
    pycb.config.redirector.end_connection(request)

def init_redirector(req, bucketName, objectName):
    pycb.log(logging.INFO, "===== def init_redirector of cumulus.py")
    redir_host = pycb.config.redirector.new_connection(req)
    req.notifyFinish().addBoth(end_redirector, req)

    if redir_host:
        pycb.log(logging.INFO, "REDIRECT %s" % (redir_host))
        ex = cbException('TemporaryRedirect')
        req.setHeader('location', "http://%s%s" % (redir_host, req.uri))
        ex.add_custom_xml("Bucket", bucketName)
        ex.add_custom_xml("Endpoint", redir_host)
        raise ex

def path_to_bucket_object(path):
    pycb.log(logging.INFO, "===== def path_to_bucket_object of cumulus.py")
    path = urllib.unquote(path)
    if path == "/":
        return (path, None)
    # extract out the bucket name
    while path[0] == '/':
        path = path[1:]
    p_a = path.split("/", 1)
    bucketName = p_a[0].strip()
    objectName = None
    # if no object do bucket operations
    if len(p_a) > 1:
        objectName = p_a[1].strip()
        if objectName == "":
            objectName = None
    return (bucketName, objectName)

def createPath(headers, path):
    pycb.log(logging.INFO, "===== def createPath of cumulus.py")

    host = headers['host']

    h_a = host.split(':')
    if len(h_a) > 0:
        host = h_a[0]

    return path

def authorize(headers, message_type, path, uri):
    pycb.log(logging.INFO, "===== def authorize of cumulus.py")

    sent_auth = headers['authorization']
    auth_A = sent_auth.split(':')
    auth_hash = auth_A[1]
    id = auth_A[0].split()[1].strip()
    user = pycb.config.auth.get_user(id)
    key = user.get_password()

    pycb.log(logging.INFO, "AUTHORIZING %s %s %s" % (message_type, path, headers))
    b64_hmac = pycb.get_auth_hash(key, message_type, path, headers, uri)

    if auth_hash == b64_hmac:
        return user
    pycb.log(logging.ERROR, "%s %s %s" % (key, b64_hmac, auth_hash))
    ec = 'AccessDenied'
    ex = cbException(ec)
    raise ex



class CBService(resource.Resource):
    isLeaf = True

    def __init__(self):
        pass

    def get_port(self):
        pycb.log(logging.INFO, "===== def get_port of cumulus.py")
        return pycb.config.port

    # amazon uses some smaller num generator, might have to match theres
    # for clients that make assumptions
    def next_request_id(self):
        pycb.log(logging.INFO, "===== def next_request_id of cumulus.py")
        return str(uuid.uuid1()).replace("-", "")

    #  figure out if the operation is targeted at a service, bucket, or


    #  object
    def request_object_factory(self, request, user, path, requestId):
        pycb.log(logging.INFO, "===== def request_object_factory of cumulus.py")
        pycb.log(logging.INFO, "path %s" % (path))
        # handle the one service operation
        if path == "/":
            if request.method == 'GET':
                cbR = cbGetService(request, user, requestId, pycb.config.bucket)
                return cbR
            raise cbException('InvalidArgument')

        (bucketName, objectName) = path_to_bucket_object(path)
        init_redirector(request, bucketName, objectName)

        pycb.log(logging.INFO, "path %s bucket %s object %s" % (path, bucketName, str(objectName)))
        if request.method == 'GET':
            if objectName == None:
                cbR = cbGetBucket(request, user, bucketName, requestId, pycb.config.bucket)
            else:
                cbR = cbGetObject(request, user, bucketName, objectName, requestId, pycb.config.bucket)
            return cbR
        elif request.method == 'PUT':
            if objectName == None:
                cbR = cbPutBucket(request, user, bucketName, requestId, pycb.config.bucket)
            else:
                args = request.getAllHeaders()
                if 'x-amz-copy-source' in args:
                    (srcBucketName, srcObjectName) = path_to_bucket_object(args['x-amz-copy-source'])
                    cbR = cbCopyObject(request, user, requestId, pycb.config.bucket, srcBucketName, srcObjectName, bucketName, objectName)
                else:
                    cbR = cbPutObject(request, user, bucketName, objectName, requestId, pycb.config.bucket)
            return cbR
        elif request.method == 'POST':
            pycb.log(logging.ERROR, "Nothing to handle POST")
        elif request.method == 'DELETE':
            if objectName == None:
                cbR = cbDeleteBucket(request, user, bucketName, requestId, pycb.config.bucket)
            else:
                cbR = cbDeleteObject(request, user, bucketName, objectName, requestId, pycb.config.bucket)
            return cbR
        elif request.method == 'HEAD' and objectName != None:
            cbR = cbHeadObject(request, user, bucketName, objectName, requestId, pycb.config.bucket)
            return cbR

        raise cbException('InvalidArgument')

    # everything does through here to localize access control
    def process_event(self, request):
        pycb.log(logging.INFO, "===== def process_event of cumulus.py")
        try:
            rPath = createPath(request.getAllHeaders(), request.path)
 
            requestId = self.next_request_id()

            pycb.log(logging.INFO, "%s %s Incoming" % (requestId, str(datetime.now())))
            pycb.log(logging.INFO, "%s %s" % (requestId, str(request)))
            pycb.log(logging.INFO, "%s %s" % (requestId, rPath))
            pycb.log(logging.INFO, "%s %s" %(requestId, request.getAllHeaders()))
            pycb.log(logging.INFO, "request URI %s method %s %s" %(request.uri, request.method, str(request.args)))

            user = authorize(request.getAllHeaders(), request.method, rPath, request.uri)
            self.allowed_event(request, user, requestId, rPath)
        except cbException, ex:
            eMsg = ex.sendErrorResponse(request, requestId)
            pycb.log(logging.ERROR, eMsg, traceback)
        except Exception, ex2:
            traceback.print_exc(file=sys.stdout)
            gdEx = cbException('InternalError')
            eMsg = gdEx.sendErrorResponse(request, requestId)
            pycb.log(logging.ERROR, eMsg, traceback)

    def allowed_event(self, request, user, requestId, path):
        pycb.log(logging.INFO, "===== def allowed_event of cumulus.py")
        pycb.log(logging.INFO, "Access granted to ID=%s requestId=%s uri=%s" % (user.get_id(), requestId, request.uri))
        cbR = self.request_object_factory(request, user, path, requestId)

        cbR.work()

    # http events.  all do the same thing
    def render_GET(self, request):
        pycb.log(logging.INFO, "===== def render_GET of cumulus.py")
        self.process_event(request)
        return server.NOT_DONE_YET

    # not implementing any post stuff for now
    def render_POST(self, request):
        pycb.log(logging.INFO, "===== def render_POST of cumulus.py")
        self.process_event(request)
        return server.NOT_DONE_YET

    def render_PUT(self, request):
        pycb.log(logging.INFO, "===== def render_PUT of cumulus.py")
        self.process_event(request)
        return server.NOT_DONE_YET

    def render_DELETE(self, request):
        pycb.log(logging.INFO, "===== def render_DELETE of cumulus.py")
        self.process_event(request)
        return server.NOT_DONE_YET
    

#class C_StringTransport(http.StringTransport):
#    
#    def writeSequence(self, seq):
#        pycb.log(logging.INFO, "===== ===== def writeSequence of cumulus.py")
#        http.StringTransport.writeSequence(self, seq)
#        
#    def __getattr__(self, attr):
#        pycb.log(logging.INFO, "===== ===== def __getattr__ of cumulus.py")
#        http.StringTransport.__getattr__(self, attr)
#
#class C_HTTPClient(http.HTTPClient):
#    
#    def sendCommand(self, command, path):
#        pycb.log(logging.INFO, "===== ===== def sendCommand of cumulus.py")
#        http.HTTPClient.sendCommand(self, command, path)
#        
#    def sendHeader(self, name, value):
#        pycb.log(logging.INFO, "===== ===== def sendHeader of cumulus.py")
#        http.HTTPClient.sendHeader(self, name, value)
#        
#    def endHeaders(self):
#        pycb.log(logging.INFO, "===== ===== def endHeaders of cumulus.py")
#        http.HTTPClient.endHeaders(self)
#        
#    def lineReceived(self, line):
#        pycb.log(logging.INFO, "===== ===== def lineReceived of cumulus.py")
#        http.HTTPClient.lineReceived(self, line)
#        
#    def connectionLost(self, reason):
#        pycb.log(logging.INFO, "===== ===== def connectionLost of class HTTPClient of cumulus.py")
#        http.HTTPClient.connectionLost(self, reason)
#        
#    def handleResponseEnd(self):
#        pycb.log(logging.INFO, "===== ===== def handleResponseEnd of cumulus.py")
#        http.HTTPClient.handleResponseEnd(self)
#        
#    def handleResponsePart(self, data):
#        pycb.log(logging.INFO, "===== ===== def handleResponsePart of cumulus.py")
#        http.HTTPClient.handleResponsePart(self, data)
#        
#    def connectionMade(self):
#        pycb.log(logging.INFO, "===== ===== def connectionMade of cumulus.py")
#        http.HTTPClient.connectionMade(self)
#        
#    def handleStatus(self, version, status, message):
#        pycb.log(logging.INFO, "===== ===== def handleStatus of cumulus.py")
#        http.HTTPClient.handleStatus(self, version, status, message)
#        
#    def handleHeader(self, key, val):
#        pycb.log(logging.INFO, "===== ===== def handleHeader of cumulus.py")
#        http.HTTPClient.handleHeader(self, key, val)
#        
#    def handleEndHeaders(self):
#        pycb.log(logging.INFO, "===== ===== def handleEndHeaders of cumulus.py")
#        http.HTTPClient.handleEndHeaders(self)
#        
#    def rawDataReceived(self, data):
#        pycb.log(logging.INFO, "===== ===== def rawDataReceived of cumulus.py")
#        http.HTTPClient.rawDataReceived(self, data)        
#    
class C_Request(http.Request):
#    
#    def __setattr__(self, name, value):
#        pycb.log(logging.INFO, "===== ===== def __setattr__ of cumulus.py")
#        http.Request.__setattr__(self, name, value)
#        
#    def _cleanup(self):
#        pycb.log(logging.INFO, "===== ===== def _cleanup of cumulus.py")
#        http.Request._cleanup(self)
#        
#    def noLongerQueued(self):
#        pycb.log(logging.INFO, "===== ===== def noLongerQueued of cumulus.py")
#        http.Request.noLongerQueued(self)
#        
#    def gotLength(self, length):
#        pycb.log(logging.INFO, "===== ===== def gotLength of cumulus.py")
#        http.Request.gotLength(self, length)
#        
#    def parseCookies(self):
#        pycb.log(logging.INFO, "===== ===== def parseCookies of cumulus.py")
#        http.Request.parseCookies(self)
#        
#    def handleContentChunk(self, data):
#        pycb.log(logging.INFO, "===== ===== def handleContentChunk of cumulus.py")
#        http.Request.handleContentChunk(self, data)
#        
#    def requestReceived(self, command, path, version):
#        pycb.log(logging.INFO, "===== ===== def requestReceived of cumulus.py")
#        http.Request.requestReceived(self, command, path, version)
        
    def requestReceived(self, command, path, version):
        pycb.log(logging.INFO, "===== ===== def requestReceived of cumulus.py")
        self.content.seek(0,0)
        self.args = {}
        self.stack = []

        self.method, self.uri = command, path
        self.clientproto = version
        x = self.uri.split('?', 1)

        if len(x) == 1:
            self.path = self.uri
        else:
            self.path, argstring = x
            self.args = parse_qs(argstring, 1)

        # cache the client and server information, we'll need this later to be
        # serialized and sent with the request so CGIs will work remotely
        self.client = self.channel.transport.getPeer()
        self.host = self.channel.transport.getHost()

        # Argument processing
        args = self.args
        ctype = self.requestHeaders.getRawHeaders('content-type')
        if ctype is not None:
            ctype = ctype[0]

        if self.method == "POST" and ctype:
            mfd = 'multipart/form-data'
            key, pdict = cgi.parse_header(ctype)
            if key == 'application/x-www-form-urlencoded':
                args.update(parse_qs(self.content.read(), 1))
            elif key == mfd:
                try:
                    args.update(cgi.parse_multipart(self.content, pdict))
                except KeyError, e:
                    if e.args[0] == 'content-disposition':
                        # Parse_multipart can't cope with missing
                        # content-dispostion headers in multipart/form-data
                        # parts, so we catch the exception and tell the client
                        # it was a bad request.
                        self.channel.transport.write(
                                "HTTP/1.1 400 Bad Request\r\n\r\n")
                        self.channel.transport.loseConnection()
                        return
                    raise

        self.process()

#        
#    def __repr__(self):
#        pycb.log(logging.INFO, "===== ===== def __repr__ of cumulus.py")
#        http.Request.__repr__(self)
#        
#    def process(self):
#        pycb.log(logging.INFO, "===== ===== def process of cumulus.py")
#        http.Request.process(self)
#        
#    def registerProducer(self, producer, streaming):
#        pycb.log(logging.INFO, "===== ===== def registerProducer of cumulus.py")
#        http.Request.registerProducer(self, producer, streaming)
#        
#    def unregisterProducer(self):
#        pycb.log(logging.INFO, "===== ===== def unregisterProducer of cumulus.py")
#        http.Request.unregisterProducer(self)
#        
#    def _sendError(self, code, resp=''):
#        pycb.log(logging.INFO, "===== ===== def _sendError of cumulus.py")
#        http.Request._sendError(self, code, resp)
#    
#    def getHeader(self, key):
#        pycb.log(logging.INFO, "===== ===== def getHeader of cumulus.py")
#        http.Request.getHeader(self, key)
#
#    def getCookie(self, key):
#        pycb.log(logging.INFO, "===== ===== def getCookie of cumulus.py")
#        http.Request.getCookie(self, key)
#        
#    def finish(self):
#        pycb.log(logging.INFO, "===== ===== def finish of cumulus.py")
#        http.Request.finish(self)
#        
#    def write(self, data):
#        pycb.log(logging.INFO, "===== ===== def write of cumulus.py")
#        http.Request.write(self, data)
#        
#    def addCookie(self, k, v, expires=None, domain=None, path=None, max_age=None, comment=None, secure=None):
#        pycb.log(logging.INFO, "===== ===== def addCookie of cumulus.py")
#        http.Request.addCookie(self, k, v, expires, domain, path, max_age, comment, secure)
#        
#    def setResponseCode(self, code, message=None):
#        pycb.log(logging.INFO, "===== ===== def setResponseCode of cumulus.py")
#        http.Request.setResponseCode(self, code, message)
#        
#    def setHeader(self, name, value):
#        pycb.log(logging.INFO, "===== ===== def setHeader of cumulus.py")
#        http.Request.setHeader(self, name, value)
#        
#    def redirect(self, url):
#        pycb.log(logging.INFO, "===== ===== def redirect of cumulus.py")
#        http.Request.redirect(self, url)
#        
#    def setLastModified(self, when):
#        pycb.log(logging.INFO, "===== ===== def setLastModified of cumulus.py")
#        http.Request.setLastModified(self, when)
#        
#    def setETag(self, etag):
#        pycb.log(logging.INFO, "===== ===== def setETag of cumulus.py")
#        http.Request.setETag(self, etag)
#        
#    def getAllHeaders(self):
#        pycb.log(logging.INFO, "===== ===== def getAllHeaders of cumulus.py")
#        http.Request.getAllHeaders(self)
#        
#    def getRequestHostname(self):
#        pycb.log(logging.INFO, "===== ===== def getRequestHostname of cumulus.py")
#        http.Request.getRequestHostname(self)
#        
#    def getHost(self):
#        pycb.log(logging.INFO, "===== ===== def getHost of cumulus.py")
#        http.Request.getHost(self)
#        
#    def setHost(self, host, port, ssl=0):
#        pycb.log(logging.INFO, "===== ===== def setHost of cumulus.py")
#        http.Request.setHost(self, host, port, ssl)
#        
#    def getClientIP(self):
#        pycb.log(logging.INFO, "===== ===== def getClientIP of cumulus.py")
#        http.Request.getClientIP(self)
#        
#    def isSecure(self):
#        pycb.log(logging.INFO, "===== ===== def isSecure of cumulus.py")
#        http.Request.isSecure(self)
#        
#    def _authorize(self):
#        pycb.log(logging.INFO, "===== ===== def _authorize of cumulus.py")
#        http.Request._authorize(self)
#        
#    def getUser(self):
#        pycb.log(logging.INFO, "===== ===== def getUser of cumulus.py")
#        http.Request.getUser(self)
#        
#    def getPassword(self):
#        pycb.log(logging.INFO, "===== ===== def getPassword of cumulus.py")
#        http.Request.getPassword(self)
#        
#    def getClient(self):
#        pycb.log(logging.INFO, "===== ===== def getClient of cumulus.py")
#        http.Request.getClient(self)
#        
#    def connectionLost(self, reason):
#        pycb.log(logging.INFO, "===== ===== def connectionLost of class Request of cumulus.py")
#        http.Request.connectionLost(self, reason)
    
#class _C_ChunkedTransferEncoding(http._ChunkedTransferEncoding):
#    
#    def dataReceived(self, data):
#        pycb.log(logging.INFO, "===== ===== def dataReceived of cumulus.py")
#        http._ChunkedTransferEncoding.dataReceived(self, data)
        
    
    

class CumulusHTTPChannel(http.HTTPChannel):
    
    def connectionMade(self):
        pycb.log(logging.INFO, "===== =====")
        pycb.log(logging.INFO, "===== =====")
        pycb.log(logging.INFO, "===== ===== def connectionMade of cumulus.py")
        http.HTTPChannel.connectionMade(self)
        
    def lineReceived(self, line):
        pycb.log(logging.INFO, "===== ===== def lineReceived of cumulus.py")
        http.HTTPChannel.lineReceived(self, line)
        
    def _finishRequestBody(self, data):
        pycb.log(logging.INFO, "===== ===== def _finishRequestBody of cumulus.py")
        #http.HTTPChannel._finishRequestBody(self, data)
        pycb.log(logging.INFO, "=====## before allContentReceived of cumulus.py")
        self.allContentReceived()
        pycb.log(logging.INFO, "=====## before setLineMode of cumulus.py")
        self.setLineMode(data)
        pycb.log(logging.INFO, "=====## after setLineMode of cumulus.py")
        
    def headerReceived(self, line):
        pycb.log(logging.INFO, "===== ===== def headerReceived of cumulus.py")
        http.HTTPChannel.headerReceived(self, line)
        
#    def allContentReceived(self):
#        pycb.log(logging.INFO, "===== ===== def allContentReceived of cumulus.py")
#        http.HTTPChannel.allContentReceived(self)
        
    def allContentReceived(self):
        pycb.log(logging.INFO, "===== ===== def allContentReceived of cumulus.py")
        command = self._command
        path = self._path
        version = self._version
        pycb.log(logging.INFO, "=====## command, path, version are %s %s %s"%(command,path,version))
        pycb.log(logging.INFO, "=====## check start of allContentReceived of cumulus.py")

        # reset ALL state variables, so we don't interfere with next request
        self.length = 0
        self._receivedHeaderCount = 0
        self.__first_line = 1
        self._transferDecoder = None
        del self._command, self._path, self._version
        #ok

        # Disable the idle timeout, in case this request takes a long
        # time to finish generating output.
        if self.timeOut:
            self._savedTimeOut = self.setTimeout(None)
        #ok

        req = self.requests[-1]
        pycb.log(logging.INFO, "=====## req is %s"%req)
        #ok
        pycb.log(logging.INFO, "=====## check of allContentReceived of cumulus.py")
        req.requestReceived(command, path, version)
        pycb.log(logging.INFO, "=====## end of allContentReceived of cumulus.py")
        
#    def rawDataReceived(self, data):
#        pycb.log(logging.INFO, "===== ===== def rawDataReceived of cumulus.py")
#        #http.HTTPChannel.rawDataReceived(self, data)
#        self.resetTimeout()
#        if self._transferDecoder is not None:
#            self._transferDecoder.dataReceived(data)
#        elif len(data) < self.length:
#            self.requests[-1].handleContentChunk(data)
#            self.length = self.length - len(data)
#        else:
#            self.requests[-1].handleContentChunk(data[:self.length])
#            self._finishRequestBody(data[self.length:])
            
#    def rawDataReceived(self, data):
#        pycb.log(logging.INFO, "===== ===== def rawDataReceived of cumulus.py")
#        #http.HTTPChannel.rawDataReceived(self, data)
#        self.resetTimeout()
#        pycb.log(logging.INFO, "=====## resetTimeout")
#        if self._transferDecoder is not None:
#            pycb.log(logging.INFO, "=====## transferDecoder")
#            self._transferDecoder.dataReceived(data)
#            pycb.log(logging.INFO, "=====## after transferDecoder")
#        elif len(data) < self.length:
#            pycb.log(logging.INFO, "=====## elif before send data")
#            self.requests[-1].content.send(data)
#            pycb.log(logging.INFO, "=====## after send data")
#            self.length = self.length - len(data)
#        else:
#            pycb.log(logging.INFO, "=====## else before send data")
#            self.requests[-1].content.send(data[:self.length])
#            self._finishRequestBody(data[self.length:])
            
    def rawDataReceived(self, data):
        pycb.log(logging.INFO, "===== ===== def rawDataReceived of cumulus.py")
        #http.HTTPChannel.rawDataReceived(self, data)
        self.resetTimeout()
        pycb.log(logging.INFO, "=====## resetTimeout")
        if len(data) < self.length:
            pycb.log(logging.INFO, "=====## elif before send data")
            self.requests[-1].content.send(data)
            pycb.log(logging.INFO, "=====## after send data")
            self.length = self.length - len(data)
        else:
            pycb.log(logging.INFO, "=====## else before send data")
            self.requests[-1].content.send(data[:self.length])
            self._finishRequestBody(data[self.length:])
        
    def checkPersistence(self, request, version):
        pycb.log(logging.INFO, "===== ===== def checkPersistence of cumulus.py")
        http.HTTPChannel.checkPersistence(self, request, version)
        
    def requestDone(self, request):
        pycb.log(logging.INFO, "===== ===== def requestDone of cumulus.py")
        http.HTTPChannel.requestDone(self, request)
        
    def timeoutConnection(self):
        pycb.log(logging.INFO, "===== ===== def timeoutConnection of cumulus.py")
        http.HTTPChannel.timeoutConnection(self)
        
    def connectionLost(self, reason):
        pycb.log(logging.INFO, "===== ===== def connectionLost of class HTTPChannel of cumulus.py")
        http.HTTPChannel.connectionLost(self, reason)
    

    def getAllHeaders(self, req):
        pycb.log(logging.INFO, "===== def getAllHeaders of cumulus.py")
        """
        Return dictionary mapping the names of all received headers to the last
        value received for each.

        Since this method does not return all header information,
        C{self.requestHeaders.getAllRawHeaders()} may be preferred.
        """
        headers = {}
        for k, v in req.requestHeaders.getAllRawHeaders():
            headers[k.lower()] = v[-1]
        return headers

    def send_access_error(self, req):
        pycb.log(logging.INFO, "===== def send_access_error of cumulus.py")
        ex = cbException('AccessDenied')
        m_msg = "HTTP/1.1 %s %s\r\n" % (ex.httpCode, ex.httpDesc)
        self.transport.write(m_msg)
        m_msg = "%s: %s\r\n" % (('x-amz-request-id', str(uuid.uuid1())))
        self.transport.write(m_msg)
        self.transport.write('content-type: text/html')
        e_msg = ex.make_xml_string(self._path, str(uuid.uuid1()))
        self.transport.write(e_msg)
        self.transport.loseConnection()

    def send_redirect(self):
        pycb.log(logging.INFO, "===== def send_redirect of cumulus.py")
        m_msg = "HTTP/1.1 %s %s\r\n" % (ex.httpCode, ex.httpDesc)
        self.transport.write(m_msg)
        m_msg = "%s: %s\r\n" % (('x-amz-request-id', str(uuid.uuid1())))
        self.transport.write(m_msg)
        self.transport.write('content-type: text/html\r\n')
        e_msg = ex.make_xml_string(self._path, str(uuid.uuid1()))
        self.transport.write(e_msg)
        self.transport.loseConnection()
        return ex


    # intercept the key event
    def allHeadersReceived(self):
        pycb.log(logging.INFO, "===== =====")
        pycb.log(logging.INFO, "===== =====")
        pycb.log(logging.INFO, "===== def allHeadersReceived of cumulus.py")
        http.HTTPChannel.allHeadersReceived(self)

        req = self.requests[-1]
        pycb.log(logging.INFO, "=====## req is %s"%req)
        req._cumulus_killed = None
        h = self.getAllHeaders(req)
        pycb.log(logging.INFO, "=====## headers is %s"%h)
        # we can check the authorization here
        rPath = self._path
        ndx = rPath.rfind('?')
        if ndx >= 0:
            rPath = rPath[0:ndx]
        rPath = createPath(h, rPath)
#        try:
#            user = authorize(h, self._command, rPath, self._path)
#        except:
#            self.send_access_error(req)
#            return

        if 'expect' in h:
            if h['expect'].lower() == '100-continue':
                self.transport.write("HTTP/1.1 100 Continue\r\n\r\n")

        (bucketName, objectName) = path_to_bucket_object(rPath)
        # if we are putting an object
        if objectName != None and self._command == "PUT":
            
            #hdfs
            if pycb.config.backend=="hdfs":
                pycb.log(logging.INFO,"=====## req.content is %s"%req.content)
                req.content.close()
                len=h["content-length"]
                pycb.log(logging.INFO,"=====## len is %s"%len)
                req.content=pycb.config.bucket.put_object(bucketName, objectName,len)
                pycb.log(logging.INFO,"=====## req.content is %s"%req.content)
                pycb.log(logging.INFO, "=====## after call put_object")
                
            elif pycb.config.backend=="posix":
                #  free up the temp object that we will not be using
                pycb.log(logging.INFO, "=====## %s"%req.content)
                req.content.close()
                # give twisted our own file like object
                req.content = pycb.config.bucket.put_object(bucketName, objectName)
                pycb.log(logging.INFO, "=====## %s"%req.content)
                pycb.log(logging.INFO, "=====## after call put_object")
                req.content.set_delete_on_close(True)
                pycb.log(logging.INFO, "=====## after call set_delete_on_close")



class CumulusSite(server.Site):
    protocol = CumulusHTTPChannel


class CumulusRunner(object):

    def __init__(self):
        self.done = False
        self.cb = CBService()
        self.site = CumulusSite(self.cb)

        # figure out if we need http of https 
        if pycb.config.use_https:
            pycb.log(logging.INFO, "using https")
            sslContext = ssl.DefaultOpenSSLContextFactory(
              pycb.config.https_key,
              pycb.config.https_cert)
            self.iconnector = reactor.listenSSL(self.cb.get_port(),
              self.site,
              sslContext)
        else:
            pycb.log(logging.INFO, "using http")
            self.iconnector = reactor.listenTCP(self.cb.get_port(), self.site)

    def getListener(self):
        pycb.log(logging.INFO, "===== def getListener of cumulus.py")
        return self.iconnector.getHost()

    def get_port(self):
        pycb.log(logging.INFO, "===== def get_port of cumulus.py")
        l = self.getListener()
        return l.port

    def run(self):
        pycb.log(logging.INFO, "===== def run of cumulus.py")
        reactor.suggestThreadPoolSize(10)
        reactor.run()

    def stop(self):
        pycb.log(logging.INFO, "===== def stop of cumulus.py")
        self.iconnector.stopListening()


def main(argv=sys.argv[0:]):
    pycb.log(logging.INFO, "===== def main of cumulus.py")
    pycb.config.parse_cmdline(argv)

    try:
        cumulus = CumulusRunner()
    except Exception, ex:
        pycb.log(logging.ERROR, "error starting the server, check that the port is not already taken: %s" % (str(ex)), tb=traceback)
        raise ex
    pycb.log(logging.INFO, "listening at %s" % (str(cumulus.getListener())))
    cumulus.run()
    return 0

if __name__ == "__main__":
    rc = main()
    sys.exit(rc)

