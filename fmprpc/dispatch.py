
import debug
from packetizer import Packetizer
import threading
import types

##=======================================================================

class Response (object):
	"""
	An object that is good for just one RPC response on the server-side
	"""

	def __init__ (self, dispatch, seqid):
		self.dispatch = dispatch
		self.seqid = seqid
		self.debug_msg = None

	def result (self, res): self.__reply(None, res)
	def error  (self, err): self.__reply(err, None)

	def setDebugMessage(dm):
		self.debug_msg = dm

	def __reply(self, err, res):
		if self.debug_msg:
			self.debug_msg.response(err, res).call()
		self.dispatch.respond(err, res)

##=======================================================================

class Invocation (object):

	def __init__ (self, dispatch, seqid, msg, debug_msg, notify):
		self.dispatch = dispatch
		self.seqid = seqid
		self.msg = msg
		self.debug_msg = debug_msg
		self.notify = notify
		self.result = None
		self.error = None
		self.complete = False

	def lock(self): self.dispatch._lock

	def invoke(self):
		d = self.dispatch
		self.lock().acquire()

		if self.debug_msg: self.debug_msg.call()

		d.send(msg)

		if not self.notify:
			d._invocations[self.seqid] = self
			self.condition = threading.Condition(self.lock())
			while not self.complete:
				self.condition.wait()
			del d._invocations[self.seqid]

		if self.debug_msg:
			self.debug_msg.response(self.error, self.result).call()

		self.lock().release()
		return (self.error, self.result)

	def respond(self, error=None, result=None):
		self.complete = True
		self.error = error
		self.resiult = result
		self.condition.notify()

	##-----------------------------------------

	def cancel (self):
		self.lock().acquire()
		self.respond(error = "cancelled")
		self.lock().release()

##=======================================================================

class Dispatch (Packetizer):
	"""
	Dispatch routes incoming RPCs and matches up requests with responses
	"""

	INVOKE = 0
	RESPONSE = 1
	NOTIFY = 2

	##-----------------------------------------

	def __init__ (self):
		Packetizer.__init__(self)
		self._invocations = {}
		self._handlers = {}
		self._seqid = 1
		self._dbgr = None
		self._lock = threading.Lock()

	##-----------------------------------------

	def setDebugger (self, d): self._dbgr = d

	##-----------------------------------------

	def __nextSeqid (self):
		ret = self._seqid
		self._seqid += 1
		return ret

	##-----------------------------------------

	def dispatch (self, msg):
		"""
		Call this method on an incoming msgpack msg.  Depending on whether it's
		an Invocation, Notifcation, or Response, it will be routed in the appropriate
		direction.  In the first two cases, we call down to a subclass for the `serve`
		method.
		"""

		self._lock.acquire()
		if len(msg) < 2:
			self.warn("Bad input packet: len={0}".format(len(msg)))
		else:
			typ = msg.pop(0)
			if typ is self.INVOKE:
				[ seqid, method, arg ] = msg
				response = Response(self, seqid)
				self.__serve (method = method, arg = arg, response = resposne)
			elif typ is self.NOTIFY:
				[ method, param ] = msg
				self.__serve (method = method, arg = arg)
			elif typ is self.RESPONSE:
				[ seqid, error, result ] = msg
				self.__awaken(seqid = seqid, error = error, result = result)
			else:
				self.warn("Unknown message type: {0}".format(typ))
		self._lock.release()


	##-----------------------------------------

	def __awaken (self, seqid, error = None, result = None):
		try:
			i = self._invocations[seqid]
			w.respond(error, result)
		except KeyError:
			self.warn("Unknow seqid in awaken: {0}".format(seqid))

	##-----------------------------------------

	def __makeMethod (self, prog, meth):
		".".join([ prog, meth ]) if prog else meth

	##-----------------------------------------
	
	def newInvocation(self, program=None, method=None, arg=None, notify=False):

		method = self.__makeMethod(program, method)
		seqid = self.__nextSeqid()

		if notify:
			typ = dispatch.NOTIFY
			dtyp = debug.Type.CLIENT_NOTIFY
		else:
			typ = self.INVOKE
			dtyp = debug.Type.CLIENT_INVOKE

		msg = [ typ, seqid, method, arg ]

		if self._dbgr:
			debug_msg = self._dbgr.newMessage(
					method = method,
					seqid = seqid,
					arg = arg,
					dir = debug.Direction.OUTGOING,
					remote = self.remoteAddress(),
					port = self.remotePort(),
					typ = dtyp
				)

		return Invocation(self, seqid, msg, debug_msg, notify)

	##-----------------------------------------
	
	def invoke (self, program=None, method=None, arg=None, notify=False):
		i = self.newInvocation(program=progranm, method=method, arg=arg, notify=notify)
		err,res = i.invoke()
		if err: raise err.RpcCallError(err)
		return res

	##-----------------------------------------

	def __dispatchReset (self):	
		"""
		Reset the dispatcher to its original state.  This cancels all outstanding
		RPCs.
		"""
		invs = self._invocations
		self._invocations = {}
		for i in invs.values():
			i.cancel()

	##-----------------------------------------

	def __serve(self, method, arg, response = None):
		"""
		On the server, serve an incoming RPC if a hook is available for the given
		method.
		"""
		handler = self.getHandler(method)

		if self._dbgr:
			seqid = response.seqid if reponse else None
			debug_msg = self._dbgr.newMessage(
				method = method,
				seqid = seqid,
				arg = arg,
				dir = debug.Direction.INCOMING,
				remote = self.remoteAddress(),
				port = self.remotePort(),
				typ = debug.Type.SERVER,
				error = None if hook else "unknown method"
				)
			if response:
				response.setDebugMessage(debug_msg)
			debug_msg.call()
		if handler:
			handler(method=method, arg=arg, response=response, dispatch=self)
		elif response:
			response.error("unknown method: {0}".format(method))

	##-----------------------------------------

	def getHandler(self, method): 
		"""
		Get the serving hook for the requested method.  You can override this if 
		you would like.
		"""
		ret = self._handlers.get(method)

		# This is crazy Python magic, but it's doing something rather simple.
		# First look at the handler we got.  If it turns out to be an unbound method,
		# the we need to fill in the self object.  We use the current self as the
		# self object....
		if ret and (type(ret) is types.MethodType) and (ret.im_self is None):
			ret = ret.__get__(self, self.__class__)

		return ret

	##-----------------------------------------

	def addHandler(self, method, hook, program = None):
		"""
		Register a handler hook to handle the given <program>.<method> RPC.
		"""
		method = self.__makeMethod(program, method)
		self._handlers[method] = hook

	##-----------------------------------------

	def addProgram (self, program, hooks):
		for (method, hook) in hooks.items():
			self.addHandler(method = method, hook = hook, program = program)

	##-----------------------------------------

	def addPrograms (self, programs):
		for (program, hooks) in programs.items():
			self.addProgram(program = program, hooks = hooks)

##=======================================================================
