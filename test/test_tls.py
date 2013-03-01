import sys
sys.path.append("../")
try:
    import tlslite
    import fmprpc.crypto.tls as tls
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
        self[uid] = tlslite.mathtls.makeVerifier(uid,pw,2048)

class Server (server.ContextualServer, threading.Thread):

    def __init__ (self, port, prog, cond):
        threading.Thread.__init__(self)
        bindto = fmprpc.OpenServerAddress(port = port)
        self.daemon = True
        self.cond = cond
        classes = { prog : P_v1 }
        server.ContextualServer.__init__(
            self, 
            classes=classes, 
            bindto=bindto)
        self.sessionCache = tlslite.SessionCache()
        self.verifier = Verifier()
        # Commit this server to accepting only TLS connections
        tls.enableServer(self)

    def tlsDoHandshake(self,tc):
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
    def stop(self):
        self.close()

@unittest.skipUnless(tlslite, "skipped since tlslite wasn't found")
class TlsTest (unittest.TestCase):
    PORT = 50001 + (int(time.time()*1000) % 1000)
    PROG = "P.1"

    @classmethod
    def setUpClass(klass):
        klass.startServer()

    @classmethod
    def startServer(klass):
        c = threading.Condition()
        c.acquire()
        t = Server(klass.PORT, klass.PROG, c)
        klass.server = t
        t.start()
        c.wait()
        c.release()

    def __call(self, p, i, t, genfn):
        arg = genfn()
        def f ():
            c = fmprpc.Client(t, self.PROG)
            res = c.invoke("reflect", arg)
            ret = True
            if (arg != res):
                ret = False
                print("Problem in Call {0}: {1} != {2}".format(i, arg, res))
            self.assertTrue(ret)
        p.push(f)

    def __runner(self, n, genfn):

        t = tls.TlsClientTransport(
            remote=fmprpc.InternetAddress(port = self.PORT),
            uid="max",
            pw="yodawg")

        ok = t.connect()
        self.assertTrue(ok)
        if ok:
            p = Pipeliner(50)
            p.start()
            for i in range(n):
                self.__call(p,i,t,genfn)
            results = p.flush()

    def test_volley_of_objects (self):
        self.__runner(200, random_object)
    def test_volley_of_strings (self):
        self.__runner(500, random_string)

    @classmethod
    def tearDownClass(klass):
        klass.server.stop()
        del klass.server

if __name__ == "__main__":
    unittest.main()
