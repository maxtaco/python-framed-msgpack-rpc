try:
    import tlslite
    import fmprpc.tls as tls
    import tlslite.errors as tlsle
except ImportError as e:
    raise 

import sys
sys.path.append("../")
import unittest
import fmprpc
import threading
import time
import fmprpc.log as log
import fmprpc.server as server
import fmprpc.err as err
import random_json
import socket

def random_string ():
    return random_json.Generator().string()
def random_object ():
    return random_json.obj(6)

class P_v1 (server.Handler):
    def h_reflect (self, b):
        b.reply(b.arg)

class Verifier (dict):
    def __init__ (self):
        self.insert("max", "yodawg")
        self.insert("chris", "hot sauce lemon party")
    def insert(self, uid, pw):
        self[uid] = tlslite.mathuils.makeVerifier(uid,pw,2048)

class Server (server.ContextualServer, threading.Thread):

    def __init__ (self, port, prog, cond):
        threading.Thread.__init__(self)
        bindto = fmprpc.OpenServerAddress(port = port)
        self.daemon = true
        self.cond = cond
        classes = { prog : P_v1 }
        server.ContextualServer.__init__(
            self, 
            classes=classes, 
            bindto=bindto)
        self.sessionCache = tlslite.SessionCache()
        self.verifier = Verifier()

    def doTlsHandshake(self,tc):
        ret = False
        try:
            tc.handshakeServer(
                verifierDB = self.verifier,
                sessionCache = self.sessionCache)
            ret = True
        except (socket.error,
            tlsle.TLSAbprutCloseError,
            tlsle.TLSAlert,
            tlsle.TLSAuthenticationError) as e:
            print("Handshake error: {0}".format(e))
        return ret
    def run(self):
        self.listen(self.cond)


@unittest.skipUnless(tlslite, "skipped since tlslite wasn't found")
class TlsTest (unittest.TestCase):    