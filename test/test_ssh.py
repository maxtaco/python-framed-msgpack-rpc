import sys
sys.path.append("../")
try:
    import paramiko
    import fmprpc.crypto.ssh as ssh
    import fmprpc.crypto.ssh_key as ssh_key
    import fmprpc.crypto.ssh_known_hosts as skh
except ImportError as e:
    pass

import sys
sys.path.append("../")
import unittest
import threading
import time
import fmprpc.log as log
import fmprpc
import fmprpc.server as server
import fmprpc.err as err
import random_json
import socket
import os
import os.path
from fmprpc.pipeliner import Pipeliner

##=======================================================================

log.Levels.setDefault(log.Levels.ERROR)
paramiko.util.log_to_file('test_ssh.log')
logo = log.newDefaultLogger(prefix="Tester")

##=======================================================================

def random_string ():
    return random_json.Generator().string()
def random_object ():
    return random_json.obj(6)

class P_v1 (server.Handler):
    def h_reflect (self, b):
        b.reply(b.arg)

##=======================================================================

AGENT_USER = os.environ["USER"]

##=======================================================================

class Server (threading.Thread, server.ContextualServer, ssh.ServerBase):

    def __init__ (self, port, prog, cond):

        # Establish treading
        threading.Thread.__init__(self)
        self.daemon = True
        self.cond = cond

        # Set up RPC routing
        classes = { prog : P_v1 }
        bindto = fmprpc.OpenServerAddress(port = port)
        server.ContextualServer.__init__(self, classes=classes, bindto=bindto)

        # Commit this server to accepting only TLS connections
        ssh.ServerBase.__init__(self)

        # Now go ahead and load in all keys needed -- both client and server ---
        # to run the experiment
        self.pubkeys = {}
        self.keyfiles = {}
        self.loadKeys()

    def loadKeys(self):
        # First load in the server key
        cwd = os.path.dirname(__file__)
        sk = os.path.join(cwd, "keys", "server_rsa")
        if not self.readRsaKey(sk):
            raise err.ServerKeyError("cannot find {0}".format(sk))

        # Also set up a known hosts registry -- a good one that has our
        # key, and a bad one without.  The bad one is just our known
        # hosts registry, which shouldn't have this jenky key for localhost
        khr = skh.KnownHostsRegistry()
        khr.add(host = "127.0.0.1", type = "ssh-rsa", key = self._key)
        self.khr_good = khr
        self.khr_bad = skh.singleton()

        # load in pubkey for id_rsa.pub (which should be loaded in via agent)
        dir = ssh_key.Dir()
        if not dir.find():
            raise err.ClientKeyError("cannot find your .ssh dir")
        keyobj = dir.key("id_rsa.pub")
        (ok,e) = keyobj.run()
        if not ok: raise err.ClientKeyError(e)
        self.pubkeys[AGENT_USER] = keyobj.key

        # load in pubkey for id_dsa.pub (which will be loaded directly from
        # the local directory
        f = os.path.join(cwd, "keys", "id_dsa")
        self.keyfiles["d"] = f
        keyobj = ssh_key.SshPrivkey(fullfile=f)
        (ok,e) = keyobj.run()
        if not ok: raise err.ClientKeyError(e)
        self.pubkeys["d"] = keyobj.key

        # For the bad guy, we're not going to register the pub key,
        # just keep the privkey for signing...
        f = os.path.join(cwd, "keys", "bad_dsa")
        self.keyfiles["b"] = f

    def sshCheckAuthPublickey(self, username, key):
        k = self.pubkeys.get(username)
        return k and k == key

    def run(self):
        self.listen(self.cond)
    def stop(self):
        self.close()

##=======================================================================

@unittest.skipUnless(paramiko, "skipped since paramiko wasn't found")
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

    def __runner(self, n, genfn, uid, key):
        t = ssh.SshClientTransport(
            remote=fmprpc.InternetAddress(port = self.PORT),
            uid=uid,
            key=key,
            known_hosts=self.server.khr_good)
        ok = t.connect()
        self.assertTrue(ok)
        if ok:
            p = Pipeliner(50)
            p.start()
            for i in range(n):
                self.__call(p,i,t,genfn)
            results = p.flush()

    def test_volley_of_objects_agent (self):
        logo.info("test_volley_of_objects_agent")
        self.__runner(200, random_object, AGENT_USER, None)
    def test_volley_of_strings_agent (self):
        logo.info("test_volley_of_strings_agent")
        self.__runner(200, random_string, AGENT_USER, None)
    def test_volley_of_strings_keyfile (self):
        logo.info("test_volley_of_strings_keyfile")
        self.__runner(50, random_string, "d", self.server.keyfiles["d"])

    def test_bad_login_bad_client_auth (self):
        logo.info("test_bad_login_bad_client_auth")
        t = ssh.SshClientTransport(
            remote=fmprpc.InternetAddress(port = self.PORT),
            uid="b",
            known_hosts=self.server.khr_good,
            key=self.server.keyfiles["b"])
        ok = t.connect()
        self.assertTrue(not ok)
        self.assertTrue(t.getError('clientAuth'))

    def test_bad_login_bad_host_auth (self):
        logo.info("test_bad_login_bad_host_auth")
        t = ssh.SshClientTransport(
            remote=fmprpc.InternetAddress(port = self.PORT),
            uid=AGENT_USER,
            known_hosts=self.server.khr_bad)
        ok = t.connect()
        self.assertTrue(not ok)
        self.assertTrue(t.getError('hostAuth'))

    @classmethod
    def tearDownClass(klass):
        klass.server.stop()
        del klass.server

##=======================================================================

if __name__ == "__main__":
    unittest.main()
