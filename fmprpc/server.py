
import listener
import re
from fmprpc.util import safepop

##=======================================================================

def collectHooks(obj, wrapper_fn = None):
	"""Collect all of the methods that start with "h_"s from the given
	object or klass. These are handler hooks and will automatically
	assume a program with this function.

	The wrapper function allows subclasses to do something interesting
	like wrap all hook functions in a try/catch block. This might
	be quite useful for catching argument errors (like missing fields
	in incoming JSON objects) in one central place.

	Note that for getting attributes on an instantiated class will result
	in bound hooks.  So the hook is bound at the time of collectHooks
	being called.  So in the case of  a simpleServer, this means the
	hook is bound to the server object, and not the connection object.
	"""

	rxx = re.compile(r'^h_(.*)$')
	hooks = {}
	for d in dir(obj):
		m = rxx.match(d)
		val = getattr(obj, d)
		if m and callable(val):
			if wrapper_fn:
				val = wrapper_fn(val)
			hooks[m.group(1)] = val
	return hooks

##=======================================================================

class Server (listener.Listener):
	"""This server is connection-centric. When the handlers of the
	passed programs are invoked, the 'this' object to the handler will
	be the Transport that's handling that client. This server is available
	via this.parent.

	Note you can pass a TransportClass to use instead of the Transport.
	It should be a subclass of Transport.

	You need to pass in a programs=programs variable to the constructor,
	which contains a dictionary that maps methods to hooks.
	"""
  
	#-----------------------------------------

	def __init__ (self, **kwargs):
		self._programs = kwargs.pop("programs")
		listener.Listener.__init__ (self, **kwargs)

	#-----------------------------------------

	def gotNewConnection (self, c):
		c.addPrograms(self._programs)

##=======================================================================

class SimpleServer (listener.Listener):
	"""
	This is a server-centric server.  The this object of the handlers
	are the parent Server class, and not the connection wrapper.
	Also, hooks are collected from the class prototype.  Those of the
	form h_* are taken to be serving hooks.

	You need to pass the constructor a program=program argument to
	tell it which program it's serving for.
	"""

	def __init__ (self, **kwargs):
		self._program = kwargs.pop('program')
		hw = safepop(kwargs, "hookWrapper")
		self.__hookWrapper = hw
		listener.Listener.__init__ (self, **kwargs)

	def gotNewConnection (self, c):
		# Note that we'll be fetching **bound** hooks from the self object,
		# so they will work fine as callables from within the dispatch without
		# any further molestation.
		hooks = collectHooks(self, self.__hookWrapper)
		c.addProgram(self.getProgramName(), hooks)

	def setProgramName(self, p): 
		self._program = p

	def getProgramName(self):
		r = self._program
		if not r:
			raise AttributeError("attribute 'program' not found")
		return r

##=======================================================================

class Handler:
	"""
	To be used with a ContextualServer --- see below.  Subclass this to
	do something that you need.
	"""

	def __init__ (self, transport, server):
		self.transport = transport
		self.server = server

##=======================================================================

class ContextualServer (listener.Listener):
	"""
	For each incoming RPC, use a subclass of Handler (above) as the 'self'
	object for a handler hook.  The handler object points to both the 
	child transport and the parent server.  Both are available as a has-a
	relationship rather than an is-a.
	"""

	def __init__ (self, **kwargs):
		self._classes = kwargs.pop("classes")
		listener.Listener.__init__(self, **kwargs)

	def gotNewConnection(self, c):
		for (key, klass) in self._classes.items():
			context = klass (transport = c, server = self)
			# Insert mappings for this program to the 
			# bound h_* hooks of the new context
			c.addProgram(key, collectHooks(context))

##=======================================================================
