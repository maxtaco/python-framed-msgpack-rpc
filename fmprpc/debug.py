
##=======================================================================

import log
import time

##=======================================================================

class Flags:
	NONE = 0x0
	METHOD = 0x1
	REMOTE = 0x2
	SEQID = 0x4
	TIMESTAMP = 0x8
	ERR = 0x10
	ARG = 0x20
	RES = 0x40
	TYPE = 0x80
	DIR = 0x100
	PORT = 0x200
	VERBOSE = 0x400
	ALL = 0xffffffff

	LEVEL_0 = NONE
	LEVEL_1 = METHOD | TYPE | DIR 
	LEVEL_2 = LEVEL_1 | SEQID | TIMESTAMP | REMOTE | PORT
	LEVEL_3 = LEVEL_2 | ERR
	LEVEL_4 = LEVEL_3 | RES | ARG

	stringFlags = {
		"m" : Flags.METHOD,
		"a" : Flags.REMOTE,
		"s" : Flags.SEQID,
		"t" : Flags.TIMESTAMP,
		"e" : Flags.ERR,
		"p" : Flags.ARG,
		"r" : Flags.RES,
		"c" : Flags.TYPE,
		"d" : Flags.DIR,
		"v" : Flags.VERBOSE,
		"P" : Flags.PORT,
		"A" : Flags.ALL,
		"0" : Flags.LEVEL_0,
		"1" : Flags.LEVEL_1,
		"2" : Flags.LEVEL_2,
		"3" : Flags.LEVEL_3,
		"4" : Flags.LEVEL_4
	}

	@classmethod
	def fromString(klass,s):
		s = "{0}".format(s)
		res = 0
		for ch in s:
			res |= klass.stringFlags[ch]
		return res

##=======================================================================

class Direction:
	INCOMING : 1
	OUTGOING : 2

##=======================================================================

def flirDir(d):
	return (Direction.INCOMING if d is Direction.OUTGOING else Direction.OUTGOING)

##=======================================================================

class Type:
	SERVER : 1
	CLIENT_NOTIFY : 2
	CLIENT_CALL : 3

##=======================================================================

F2S = {
	Flags.DIR : {
		Direction.INCOMING : "in",
		Direction.OUTGOING : "out"
	},
	Flags.TYPE : {
		Type.SERVER : "server",
		Type.CLIENT_CALL : "client.invoke",
		Type.CLIENT_NOTIFY : "client.notify"
	}
}

##=======================================================================

class Debugger (object):

	def __init__ (self, flags, log_obj=None, log_hook=None):
		self.flags = Flags.fromString(flags) if (type(flags) is str) else flags
		self.log_obj = log_obj if log_obj else log.newDefaultLogger()
		self.log_hook = log_hook if log_hook else self.log_obj.info

	def newMessage(self, **kwargs):
		return Message(kwargs, self)

	def __output (self, json_msg):
		self.log_hook(repr(json_msg))

	def __skipFlag (self, f):
		return (f & (Flags.PORT | Flags.REMOTE))

	def call (self,msg):
		new_json_msg = {}
		V = self.flags & Flags.VERBOSE
		if (self.flags & Flags.TIMESTAMP):
			new_json_msg.timestamp = time.time()
		for (key,val) in msg.toJsonObject().items():
			uck = key.upper()
			flag = getattr(Flags,uck)

			if self.__skipFlag(flag):
				do_copy = False
			elif not (self.flags & flag):
				do_copy = False
			elif key is "arg":
				do_copy = msg.showArg(V)
			elif key is "res":
				do_copy = msg.showRes(V)
			else:
				do_copy = True

			if do_copy:
				f2s = F2S[flag]
				if f2s: val = f2s[val]
				new_json_msg[key] = val

		self.__output(new_json_msg)

##=======================================================================

class Message (object):
	"""
	A debug message --- a wrapper around a map object with a few
	additional methods.
	"""

	def __init__ (self, msg={}, debugger=None):
		self.msg = msg
		self.debugger = debugger

	def response (error, result):
		self.msg['err'] = error
		self.msg['res'] = result
		self.msg['dir'] = flipDir self.msg['dir']
		return self

	def toJsonObject (self): return self.msg
	def call(self) : self.debugger.call(self.msg)
	def set(self,k,v): self.msg[k] = v

	def isServer(self): return (self.msg.type is Type.SERVER)
	def isClient(self): return not self.isServer()
	def isIncoming(self): return (self.msg.dir is Direction.INCOMING)
	def isOutgoing(self): return (self.msg.dir is Direction.OUTGOING)

	def showArg (self, V):
		return (V or (self.isServer() and self.isIncoming()) or
				     (self.isClient() and self.isOutgoing()))
	def showRes (self, V):
		return (V or (self.isServer() and self.isOutgoing()) or
				     (self.isClient() and self.isIncoming()))

##=======================================================================

def makeDebugger (flags, log_obj):
	return (None if (flags is 0) else Debugger(flags = flags, log_obj = log_obj))

##=======================================================================






