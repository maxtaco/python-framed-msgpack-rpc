import socket
import log
import dispatch
import debug
import threading
import time
import ilist

##=======================================================================

class ClearStreamWrapper (object):
	"""
	A shared wrapper around a socket, for which close() is idempotent. Of course,
	no encyrption on this interface.
	"""
	def __init__ (self, socket, parent):
		# Disable Nagle by default on all sockets...
		socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
		self.socket = socket
		self.parent = parent
		self.generation = parent.__nextGeneration()
		self.write_closed_warn = False

	def close (self):
		"""
		Return True if we did the actual close, and false otherwise
		"""
		ret = False
		if self.socket:
			ret = True
			x = self.socket
			self.socket = None
			self.parent.__dispatchReset()
			self.parent.__packetizerReset()
			x.close()
		return ret

	def write (self, msg):
		"""
		Write the message to the socket, calling send(2) repeatedly
		until the buffer is flushed out.  Use low-level socket calls.
		"""
		if self.socket:
			self.socket.sendall(msg)
		elif not self.write_closed_warn:
			self.write_closed_warn = True
			self.parent._warn("write on closed socket")

	def stream (self): return self.socket
	def isConnected (self): return not not self.socket
	def getGeneration (self): return self.generation

	def remoteAddress (self):
		return self.socket.getpeername()[0] if self.socket else None
	def reportPort (self):
		return self.socket.getpeername()[1] if self.socket else None
	def remotePeer (self):
		return self.socket.getpeername() if self.socket else None

##=======================================================================

class Transport (dispatch.Dispatch):

	def __init__ (self, port=None, host=None, tcp_opts={}, 
				  stream=None, log_obj=None, parent=None,
				  hooks={}, dbgr=None):

		self._host = "localhost" if (not host or host is "-") else host
		self._port = port
		self._tcp_opts = tcp_opts
		self._explicit_close = False
		self._remote_str = ":".join([self.host,port])

		self.setLogger(log_obj)
		self._generation = 1
		self._lock = threading.Lock()
		self._dbgr = dbgr
		self._hooks = hooks
	
		# We don't store the TCP stream directly, but rather, sadly,
		# a **wrapper** around the TCP stream.  This is to make stream
		# closes idempotent. The case we want to avoid is two closes
		# on us (due to errors), but the second closing the reconnected
		# stream, rather than the original stream.  This level of
		# indirection solves this.
		self._stream_w = None

		self._node = ilist.Node()

		# potentially set @_tcpw to be non-null
		if stream:
			self.__activateStream(stream)

	##-----------------------------------------

	def serverListNode (self): return self._node

	##-----------------------------------------

	def setDebugger (d): self._dbgr = d
  
	##---------------------------------------

	def setDebugFlags (f):
		self.setDebugger(debug.makeDebugger(d,self._log_obj))
   
	##-----------------------------------------

	def __nextGeneration (self):
		"""To be called by StreamWrapper objects but not by
		average users."""
		ret = self._generation
		self._generation += 1
		return ret
 
	##-----------------------------------------

	def getGeneration (self):
		return self._stream_w.getGeneration() if self._stream_w else -1 
 
	##-----------------------------------------

	def remoteAddress (self):
		return self._stream_w.remoteAddress() if self._stream_w else None
	def remotePort (self):
		return self._stream_w.remotePort() if self._stream_w else None
	def remotePeer (self):
		return self._stream_w.remotePeer() if self._stream_w else None

	##-----------------------------------------

	def setLogger (o):
		if not o:
			o = log.newDefaultLoggger()
		self._log_obj = o
		self._log_obj.setRemote(self._remote_str)

	##-----------------------------------------

	def getLogger (self): return self._log_obj

	##-----------------------------------------

	def isConnected (self):
		if self._stream_w: return self._stream_w.isConnected()
		else: 	           return False

	##-----------------------------------------

	def connect (self):
		self._lock.acquire()
		if not self.isConnected():
			ret = self.__connectCriticalSection()
		else:
			ret = True
		self._lock.release()
		if not ret:
			self.__reconnect(True)
		return ret

	##-----------------------------------------

	def __reconnect(self, first):
		"""
		The standard transport won't try to reconnect....
		"""
		return False

	##-----------------------------------------

	def reset(self, w):
		if not w: w = self._stream_w
		self.__close(w)

	##-----------------------------------------

	def close (self):
		"""
		Call to explicitly close this connection.  After an explicit close,
		reconnects are not attempted....
		"""
		self._explicit_close = True
		if self._stream_w:
			w = self._stream_w
			self._stream_w = None
			w.close()
	#
	# /Public API
	##---------------------------------------------------

	def __warn(e) : self._log_obj.warn(e)
	def __info(e) : self._log_obj.info(e)
	def __fatal(e): self._log_obj.fatal(e)
	def __debug(e): self._log_obj.debug(e)
	def __error(e): self._log_obj.error(e)
  
	##-----------------------------------------

	def __close (self, tcpw):
		# If an optional close hook was specified, call it here...
		if (self._hooks and self._hooks.eof):
			self._hooks.eof(tcpw)
		if tcpw.close():
			self.__reconnect(False)

	##-----------------------------------------

	def __handleError(self, e, tcpw):
		self.__error(e)
		self.__close(tcpw)

	##-----------------------------------------
 
	def __packetizeError (self, err): 
		# I think we'll always have the right TCP stream here
		# if we grab the one in the this object.  A packetizer
		# error will happen before any errors in the underlying
		# stream
		self.__handleError("In packetizer: {0}".format(err), self._stream_w)
	
	##-----------------------------------------

	def __handleClose (self, tcpw):
		if not self._explicit_close:
			self.__info("EOF on transport")
			self.__close(tcpw)

		# for TCP connections that are children of Listeners,
		# we close the connection here and disassociate
		if self._parent:
			self._parent.closeChild(self)
   
	##-----------------------------------------
  
	def __activateStream (self, x):

		self.__info("connection established")

		# The current generation needs to be wrapped into this hook;
		# this way we don't close the next generation of connection
		# in the case of a reconnect....
		w = ClearStreamWrapper(x, self)
		self._stream_w = w

		# If optional hooks were specified, call them here; give as an
		# argument the new StreamWrapper so that way the subclass can
		# issue closes on the connection
		if self._hooks and self._hooks.connected:
			self._hooks.connected(w)

		self.__readLoop()

	##-----------------------------------------

	def __readLoop (self):
		"""
		Keep reading from a stream until there is an EOF or an error.
		"""

		self._lock.acquire()
		go = True

		w = self._stream_w

		while go and w:
			try:
				buf = w.recv()
				if buf:
					self.packetizeData(buf)
				else:
					self.__handleClose(w)
					go = False
			except IOError as e:
				self.__handleError(e, w)
				go = False

		self._lock.release()

	##-----------------------------------------

	def __connectCriticalSection(self):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		ok = False
		try:
			s.connect((self.host, self.port))
			self.__activateStream(s)
			ok = True
		except socket.error as e:
			self.__warn("Error in connection to {0}: {1}"
				.format(self._remote_str, e))
		return ok

	##-----------------------------------------
	# To fulfill the packetizer contract, the following...
  
	def rawWrite (msg, encoding):
		if not self._stream_w:
			self.__warn("write attempt with no active stream")
		else:
			self._stream_w.write(msg)
 

##=======================================================================

class RobustTransport (Transport):
	"""
	Takes the same parameters as Transport above, but also
	some additionals to tweak the behavior of the 'robust'
	features:
  
	reconnect_delay -- the number of seconds to delay between attempts
		to reconnect to a downed server.
   
	queue_max -- the limit to how many calls we'll queue while we're
		waiting on a reconnect.
   
	warn_threshhold -- if a call takes more than this number of seconds,
		a warning will be fired when the RPC completes.
  
	error_threshhold -- if a call *is taking* more than this number of
		seconds, we will make an error output while the RPC is outstanding,
		and then make an error after we know how long it took.
	"""

	def __init__ (self, port=None, host=None, tcp_opts={}, stream=None, log_obj=None,
				  parent=None, hooks={}, dbgr=None, 
				  reconnect_delay=1, queue_max=1000,
				  warn_threshhold=3.0, error_threshhold=10.0):

		Transport.__init__(self, port=port, host=host,
			tcp_opts=tcp_opts, stream=stream, log_obj=log_obj,
			parent=parent, hooks=hooks, dbgr=dbgr)

		self._queue_max = queue_max
		self._reconnect_delay = reconnect_delay
		self._warn_threshhold = warn_threshhold
		self._error_threshhold = error_threshhold
		self._time_rpcs = (self.warn_treshhold or self._error_threshhold)
		self._condition = threading.Condition()
   
	##-----------------------------------------

	def __pokeQueue (self):
		self._condition.acquire()
		self._condition.notifyAll()
		self._condition.release()

	##-----------------------------------------

	def __reconnect(self, first_time):
		if not self._explicit_close:
			self.__connectLoop(first_time)

	##-----------------------------------------

	def __connectLoop(self, first_time):
		prfx = "" if first_time else "re"
		i = 0

		self._lock.acquire() 

		go = True
		while go:
			i += 1
			if self.isConnected() or self._explicit_close:
				go = False
			else:
				self.__info("{0}connecting (attempt {1})".format(prfx, i))
				ok = self.__connectCriticalSection()
				if ok:
					go = False
				else:
					time.sleep(self._reconnect_delay)

		if self.isConnected():
			s = "" if (i is 1) else "s"
			self.__warn("{0}connected after {1} attempt{2}".format(prfx, i, s))   
			self.__pokeQueue()

		self._lock.release()

	##-----------------------------------------

	def __timedInvoke (self, program, method, arg, notify):

		eth = self._error_threshhold
		wth = self._warn_threshhold
		timer = None
		m = self.makeMethod(program,method)

		def __do_timeout():
			if inv:
				self.__error("RPC call to '{0}' is taking > {1}s".format(m, eth))
				timer = None

		if eth:
			timer = threading.Timer(eth, __do_timeout)

		start = time.time()
		inv = self.newInvocation(program=program, method=method, arg=arg, notify=notify)
		ret = inv.call()
		dur = time.time() - start

		if eth and eth <= dur: fn = self.__error
		elif wth and wth <= dur: fn = self.__warn
		else: fn = None

		if fn:
			fn("RPC call to '{0}' finished in {1}s".format(m, dur))

		if timer:
			timer.cancel()

		return ret

	##-----------------------------------------

	def __waitInQueue(self):
		self._condition.acquire()
		self._n_waiters += 1
		self._condition.wait()
		self._n_waiters -= 1
		self._condition.release()

	##-----------------------------------------

	def invoke(self, **kwargs):
		meth = self.makeMethod(kwargs.get("program"), kwargs.get("method"))
		ret = None
		go = True
		while go:
			go = False
			if self.isConnected():
				if self._time_rpcs:
					ret = self.__timedInvoke(**kwargs)
				else:
					ret = Transport.invoke(self, **kwargs)
			elif self._explicit_close:
				self.__warn("Invoked call to '{0}' after explicit close".format(meth))
			elif self._n_waiters < self._queue_max:
				self.__waitInQueue()
				go = True
			else:
				self.__warn("Queue overflow at '{0}'".format(meth))
		return ret
  
##=======================================================================

def createTransport(**kwargs):
	if kwargs.get('robust'):
		ret = RobustTransport(**kwargs)
	else:
		ret = Transport(**kwargs)
	return ret

##=======================================================================

