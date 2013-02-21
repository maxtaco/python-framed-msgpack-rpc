import socket
import transport
import log
import debug
import ilist
import time
import util
import sys

##=======================================================================

class Listener (object):

    def __init__(self, bindto, TransportClass=None, log_obj=None):
        self.bindto = bindto
        self.TransportClass = TransportClass if TransportClass else transport.Transport
        self.setLogger(log_obj)
        self._children = ilist.List()
        self._dbgr = None

    def __defaultLogger(self):
        l = log.newDefaultLogger()
        l.setPrefix("RPC-server")
        l.setRemote(self.bindto)
        return l

    def setDebugger (self, d) :
        self._dbgr = d

    def setLogger (self, o):
        if not o:
            o = self.__defaultLogger()
        self._log_obj = o

    def setDebugFlags (self, f, apply_to_children):
        self.setDebugger(debug.makeDebugger(f, self._log_obj))
        if apply_to_children:
            self._children.walk(lambda x: x.setDebugFlags(f))

    def makeNewTransport (self, c, remote):
        """
        Given an incoming TCP stream c of type socket, wrap it with the appropriate
        transport wrapper. Virtualize this as you please. By default, it will
        use the TransportClass in the Listener Object.
        """

        # Note that we don't want to start listening on the stream yet, so 
        # we don't activate until after we install the handlers....
        x = self.TransportClass(
            remote = remote,
            parent = self,
            log_obj = self.makeNewLogObject(remote),
            dbgr = self._dbgr
            )
        self._children.push(x.serverListNode()) 
        return x

    def __gotNewConnection(self, c, remote):
        x = self.makeNewTransport(c, remote)
        self.gotNewConnection(x)
        x.activateStream(c)
        self.info("JJ rc={0}".format(sys.getrefcount(x)))

    def gotNewConnection(self, c):
        raise NotImplementedError("Listener::gotNewConnection is pure virtual")

    def makeNewLogObject (self, remote):
        return self._log_obj.makeChild(remote=remote)

    def removeChild (self, c):
        self._children.remove(c.serverListNode())

    def close(self):
        if self._tcp_server:
            x = self._tcp_server
            self._tcp_server = None
            x.close()

    def setPort (self, p): self.port = p

    def error(self,msg): self._log_obj.error(msg)
    def warn(self,msg) : self._log_obj.warn(msg)
    def info(self,msg) : self._log_obj.info(msg)

    def __bindAndListen(self, ql):
        # This code taken: from http://docs.python.org/2/library/socket.html
        # The idea is to work properly in an IPv6 environment, but only
        # if that's preferred.
        (host,port) = tuple(self.bindto)
        addrs = socket.getaddrinfo(host, port, socket.AF_UNSPEC,
                                   socket.SOCK_STREAM, 0, socket.AI_PASSIVE)
        for res in addrs:
            af, socktype, proto, canonname, sa = res
            try:
                s = socket.socket(af, socktype, proto)
            except socket.error as msg:
                self.error("Failure in socket allocation: {0}".format(msg))
                s = None
            if s:
                try:
                    s.bind(sa)
                    s.listen(ql)
                    return s
                except socket.error as msg:
                    self.error("Could not bind to address {0}: {1}"
                                  .format(str(self.bindto), msg))
                    s.close()
                    s = None
        return None

    def listen (self, cond=None, queue_len=1000):
        """
        Bind to the host/port given in the object's constructor, and set up a listen
        queue of the given size.
        """
        s = self.__bindAndListen(queue_len)
        ok = False
        if s:
            ok = True
            self._tcp_server = s
            if cond:
                cond.acquire()
                cond.notify()
                cond.release()
            self.__listenLoop()
        return ok

    def __listenLoop (self):
        while True:
            try:
                sock, addr = self._tcp_server.accept()
                self.__gotNewConnection(sock, util.InternetAddress(tup=addr))
            except socket.error as e:
                self.warn("Accept error: {0}".format(e))
        self.info("Leaving listen loop")

    def listenRetry (self, delay, cond=None, queue_len=1000):
        """
        Like listen(), but keep retrying if the binding failed --- maybe
        because another process hasn't let go of the port yet.
        Wait delay seconds between each attempt to rebind.
        """
        ok = False
        while not ok:
            ok = self.listen(cond=cond, queue_len=queue_len)
            if not ok:
                time.sleep(delay)

##=======================================================================

