

class Client (object):

	def __init__ (self, transport, program = None):
		self._transport = transport
		self._program = program

	def invoke (self, method, arg):
		return self._transport.invoke(program=self.program, 
			method=method, arg=arg, notify=False)

	def notify (self, method, arg):
		return self._transport.invoke(program=self.program, 
			method=method, arg=arg, notify=True)
