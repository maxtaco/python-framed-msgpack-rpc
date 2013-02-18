
import debug
from packetizer import Packetizer
import threading

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
		self.res = None
		self.err = None
		self.complete = False

	def lock(self): self.dispatch._lock

	def call (self):
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
			self.debug_msg.response(self.err, self.res).call()

		self.lock().release()
		return [ self.err, self.res ]

	def respond(self,err,res):
		self.complete = True
		self.err = err
		self.res = res
		self.condition.notify()

##=======================================================================

class Dispatch (Packetizer):
	"""
	Dispatch routes incoming RPCs and matches up requests with responses
	"""

	INVOKE = 0
	RESPONSE = 1
	NOTIFY = 2

	def __init__ (self):
		Packetizer.__init__(self)
		self._invocations = {}
		self._handlers = {}
		self._seqid = 1
		self._dbgr = None
		self._lock = threading.Lock()

	def setDebugger (self, d): self._dbgr = d

	def dispatch (self, msg):
		self._lock.acquire()
		if len(msg) < 2:
			self.__warn("Bad input packet: len={0}".format(len(msg)))
		else:
			typ = msg.pop(0)
			if typ is self.INVOKE:
				[ seqid, method, param ] = msg
				response = Response(self, seqid)
				self.serve (method = method, param = param, response = resposne)
			elif typ is self.NOTIFY:
				[ method, param ] = msg
				self.serve (method = method, param = param)
			elif typ is self.RESPONSE:
				[ seqid, error, result ] = msg
				self.__dispatchHandleReponse(seqid = seqid, error = error, result = result)
			else:
				self.__warn("Unknown message type: {0}".format(typ))
		self._lock.release()

	def __dispatchHandleResponse (self, seqid, error = None, result = None):
		self.__awaken(seqid = seqid, error = error, result = result)

	def cancel (self, seqid): 
		self._lock.acquire()
		self.__awaken (seqid = seqid, error = "cancelled")
		self._lock.release()

	def __awaken (self, seqid, error = None, result = None):
		try:
			i = self._invocations[seqid]
			w.respond(error, result)
		except KeyError:
			self.__warn("Unknow seqid in awaken: {0}".format(seqid))

	def invoke(self, program=None, method=None, arg=None, notify=False, call=True):

		method = self.__makeMethod(program, method)
		seqid = self.__nextSeqid()

		if notify:
			typ = dispatch.NOTIFY
			dtyp = debug.constants.type.CLIENT_NOTIFY
		else:
			typ = self.INVOKE
			dtyp = debug.constants.type.CLIENT_INVOKE

		msg = [ typ, seqid, method, arg ]

		if self._dbgr:
			debug_msg = self._dbgr.newMessage(
					method = method,
					seqid = seqid,
					arg = arg,
					dir = debug.constants.dir.OUTGOING,
					remote = self.remoteAddress(),
					port = self.remotePort(),
					typ = dtyp
				)

		i = Invocation(self, seqid, msg, debug_msg, notify)
		if call:
			ret = i.invoke()
		else:
			ret = i
		return ret


