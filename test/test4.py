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
from fmprpc.pipeliner import Pipeliner

def random_string ():
    return random_json.Generator().string()
def random_object ():
    return random_json.obj(6)

class P_v1 (server.Handler):
    def h_reflect (self, b):
        b.reply(b.arg)

class ServerThread(threading.Thread):
    def __init__ (self, port, prog, cond):
        threading.Thread.__init__(self)
        bindto = fmprpc.OpenServerAddress(port = port)
        self.srv = server.ContextualServer(
            bindto = bindto,
            classes = { prog : P_v1 }
        )
        self.daemon = True
        self.cond = cond

    def run(self):
        self.srv.listenRetry(2,self.cond)

    def stop(self):
        self.srv.close()

class Test4(unittest.TestCase):

    PORT = 50004
    PROG = "P.1"

    @classmethod
    def setUpClass(klass):
        klass.startServer()

    @classmethod
    def startServer(klass):
        c = threading.Condition()
        c.acquire()
        t = ServerThread(klass.PORT, klass.PROG, c)
        klass.server_thread = t
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
        t = fmprpc.Transport(remote = fmprpc.InternetAddress(port = self.PORT))
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
        klass.server_thread.stop()
        del klass.server_thread

if __name__ == "__main__":
    unittest.main()
