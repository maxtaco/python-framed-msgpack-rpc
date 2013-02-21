import sys
sys.path.append("../")
import unittest
import fmprpc
import threading
import time
import fmprpc.log as log
import sys

log.Levels.setDefault(log.Levels.WARN)


class ServerThread(threading.Thread):

    def __init__ (self, port, prog, cond):
        threading.Thread.__init__(self)
        self.daemon = True
        self.port = port
        self.cond = cond
        self.prog = prog

    def launchServer(self):
        s = fmprpc.Server(
            bindto = fmprpc.OpenServerAddress(port = self.port),
            programs = {
                self.prog : {
                    "foo" : lambda b: b.reply({ "y" : b.arg["i"] + 2 }),
                    "bar" : lambda b: b.reply({ "y" : b.arg["j"] * b.arg["k"] })
                }

            })
        self.server = s
        return s.listen(self.cond)

    def run(self):
        self.launchServer()

    def stop(self):
        self.server.close()


class Test1(unittest.TestCase):

    PORT = 50000 + (int(time.time()) % 1000)
    PROG = "P.1"

    @classmethod 
    def setUpClass(klass):
        c = threading.Condition()
        c.acquire()
        t = ServerThread(klass.PORT, klass.PROG, c)
        klass.server_thread = t
        t.start()
        # Wait for the thread to signal that it's ready to serve....
        c.wait()
        c.release()

    def __simple(self):
        t = fmprpc.Transport(remote = fmprpc.InternetAddress(port = self.PORT))
        ok = t.connect()
        self.assertTrue(ok)
        if ok:
            c = fmprpc.Client(t, self.PROG)
            arg = { "i" : 4 }
            res = c.invoke("foo",arg)
            self.assertEquals(res["y"], 6)
            arg = { "j" : 7, "k" : 11 }
            res = c.invoke("bar",arg)
            self.assertEquals(res["y"], 77)

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
