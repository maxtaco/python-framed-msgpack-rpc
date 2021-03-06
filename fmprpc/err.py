

class Error(Exception):
	def __init__ (self, value):
		self.value = value
	def __str__ (self):
		return repr(self.value)

class RingError (Error): pass
class UnpackTypeError(Error): pass
class RpcCallError(Error): pass
class PipelinerError(Error): pass
class AuthenticationError(Error): pass
class DeadTransportError(Error): pass
class ServerKeyError(Error): pass
class ClientKeyError(Error): pass
class LogError(Error): pass