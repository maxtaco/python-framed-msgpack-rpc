

class Error(Exception):
	def __init__ (self, value):
		self.value = value
	def __str__ (self):
		return repr(self.value)

class RingError (Error): pass
class UnpackTypeError(Error): pass
class RpcCallError(Error): pass

