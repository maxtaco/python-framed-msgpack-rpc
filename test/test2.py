
import sys
sys.path.append("../")
import unittest
import fmprpc
import threading
import time
import fmprpc.log as log
import fmprpc.server as server
import fmprpc.err as err


class P_v1 (server.Handler):
    def h_foo (self, b):
        b.reply({ "y" : b.arg["i"] + 2 })
    def h_bar (self, b):
        b.reply({ "y" : b.arg["j"] * b.arg["k"]})

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
        self.srv.listen(self.cond)

    def stop(self):
        self.srv.close()

class Test2(unittest.TestCase):

    PORT = 50001 + (int(time.time()) % 1000)
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

    def __simple(self):
        t = fmprpc.Transport(remote = fmprpc.InternetAddress(port = self.PORT))
        ok = t.connect()
        self.assertTrue(ok)
        if ok:
            c = fmprpc.Client(t, self.PROG)
            arg = { "i" : 4 }
            res = c.invoke("foo", arg)
            self.assertEquals(res["y"], 6)
            arg = { "j" : 7, "k" : 11 }
            res = c.invoke("bar", arg)
            self.assertEquals(res["y"], 77)
            bad = "XXyyXX"
            try:
                res = c.invoke(bad, arg)
                self.assertTrue(False)
            except err.RpcCallError as e:
                self.assertTrue(str(e).find("unknown method") >= 0)
                self.assertTrue(str(e).find(bad) >= 0)

    def test_a (self):
        self.__simple()
    def test_b (self):
        self.__simple()

    @classmethod
    def tearDownClass(klass):
        klass.server_thread.stop()
        del klass.server_thread

if __name__ == "__main__":
    unittest.main()
