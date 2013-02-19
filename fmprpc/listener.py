
import socket
import transport
import log
import debug
import list

##=======================================================================

class Listener (object):

	def __init__(self, port, host, TransportClass, log_obj):
		self.TransportClass = TransportClass if TransportClass else transport.Transport
		self.setLogger(lob_obj)
		self._children = list.List()
