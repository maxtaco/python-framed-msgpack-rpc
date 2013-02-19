
import socket
import transport
import log
import debug
import ilist
import time

##=======================================================================

class Listener (object):

	def __init__(self, port, host, TransportClass, log_obj):
		self.port = port
		self.host = host
		self.TransportClass = TransportClass if TransportClass else transport.Transport
		self.setLogger(lob_obj)
		self._children = ilist.List()

	def __defaultLogger(self):
		l = log.newDefaultLogger()
		l.setPrefix("RPC-server")
		h = self.host if self.host else "0.0.0.0" 
		l.setRemote(":".join([h, self.port]))
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

	def makeNewTransport (self, c, addr):
		"""
		Given an incoming TCP stream c of type socket, wrap it with the appropriate
		transport wrapper. Virtualize this as you please. By default, it will
		use the TransportClass in the Listener Object.
		"""
		x = self.TransportClass(
			stream = c,
			host = addr[0],
			port = addr[1], 
			parent = self,
			log_obj = self.makeNewLogObject(c),
			dbgr = self._dbgr
			)
		self._children.push(x.serverListNode())	
		return x

	def __gotNewConnection(self, c, addr):
		x = self.makeNewTransport(c, addr)
		self.gotNewConnection(x)

	def gotNewConnection(self, c):
		raise NotImplementedError("Listener::gotNewConnection is pure virtual")

	def makeNewLogObject (self, c):
		r = ":".join([ str(i) for i in c.getpeername() ])
		return self._log_obj.makeChild(prefix = "RPC", remote=r)

	def closeChild (self, c):
		self._children.remove(c.serverListNode())

	def setPort (self, p): self.port = p

	def __bindAndListen(self, ql):

		# This code taken: from http://docs.python.org/2/library/socket.html
		# The idea is to work properly in an IPv6 environment, but only
		# if that's preferred.
		addrs = socket.getaddrinfo(self.host, self.port, socket.AF_UNSPEC,
                              socket.SOCK_STREAM, 0, socket.AI_PASSIVE)
		for res in addres:
		    af, socktype, proto, canonname, sa = res
    		try:
        		s = socket.socket(af, socktype, proto)
		    except socket.error as msg:
		    	s.__error("Failure in socket allocation: {0}".format(msg))
    		    s = None
		        continue
		    try:
    		    s.bind(sa)
        		s.listen(ql)
		    except socket.error as msg:
		    	s.__error("Could not bind to port {0}: {1}".format(self.port, msg))
		        s.close()
		        s = None
		        continue
    		break
    		return s

	def listen (self):
		# A big queue of 1000 backloggers....
		s = self.__bindAndListen(1000)
		ok = False
		if s:
			ok = True
			self._tcp_server = s
			self.__listenLoop()
		return ok

	def __listenLoop (self):
		while True:
			sock, addr = self._tcp_server.accept()
			self.__gotNewConnection(sock, addr)

	def listenRetry (self, delay):
		ok = False
		while not ok:
			ok = self.listen()
			if not ok:
				time.sleep(delay)

##=======================================================================

