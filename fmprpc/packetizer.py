
import msgpack
from msgpack.exceptions import UnpackValueError
from ring import Ring
from err import UnpackTypeError

##=======================================================================

def unpackType(buf, typ):
	ret = msgpack.unpackb(buf)
	got = type(ret)
	if got != typ:
		raise UnpackTypeError("wrong type: got {0}; wanted {1}".
			format(got, typ))
	return ret

##=======================================================================

def __msgpackFrameLen (byt) :
	"""
	This is a hack of sorts. Given the leading byte of a frame header is,
	output the length of the frame header
	"""
	if byt < 0x80 : return 1
	if byt is 0xcc: return 2
	if byt is 0xcd: return 3
	if byt is 0xce: return 5
	return 0

##=======================================================================

class Packetizer (object):
	"""
	A packetizer that is used to read and write to an underlying
	stream (like a Transport). Should be inherited by such a class.
	The subclass should implement:

		rawWrite(msg) --- write this msg to the stream.
		   Typically handled at the Transport level (2 classes higher)

		packetizeError(e) --- report an error with the stream.
		   Typically handled by the dispatcher (1 class higher).

		dispatch(msg) --- emit a packetized imcoming message.
			Typically handled by the Dispatcher (1 class higher)

	The subclass should call packetizeData(m) whenever it has
	data to stuff in to the packetizer's input path, and call
	send(m) whenever it wants to stuff data into the packetizer's
	output path.
	"""

	#-------------------------------

	# The two states we can be in
	FRAME = 1
	DATA = 2

	# Results of getting
	OK = 0
	WAIT = 1
	ERR = -1

	#-------------------------------

	def __init__ (self):
		self._ring = Ring()
		self._state = self.FRAME
		self._next_msg_len = 0

	#-------------------------------

	def send (self, msg):
		b2 = msgpack.packb(msg)
		b1 = msgpack.packb(len(b2))
		self.rawWrite(b1)
		self.rawWrite(b2)

	#-------------------------------

	def __getFrame (self):
		"""
		Internal method: get the frame part of a stream.
		"""

		if len(self._ring) is 0: return self.WAIT
		f0 = self._ring.grab(1)
		if not f0: return self.WAIT

		frame_len = __msgpackFrameLen(f0)
		if not frame_len:
			self.packetizeError("Bad frame header received")
			return self.ERR

		f_full = self._ring.grab(frame_len)
		if not f_full: return self.WAIT

		try:
			r = unpackType(f_full, int)
			self._ring.consume(frame_len)
			self._next_msg_len = r
			self._state = self.DATA
			return self.OK
		except UnpackValueError:
			self.packetizeError("Bad decoding in frame header; unpackb failed")
		except UnpackTypeError as e:
			self.packetizeError("Bad data type in frame header: {0}".format(e))
		return self.ERR

	#-------------------------------

	def __getPayload(self):
		"""
		Internal method: get the msg part of the stream.
		"""
		l = self._next_msg_len
		if l > len(self._ring): return self.WAIT
		buf = self._ring.grab(l)
		if not buf: return self.WAIT

		try:
			msg = unpackType(buf, list)
			self._ring.consume(l)
			self._state = self.FRAME
			self.dispatch(msg)
			return self.OK
		except UnpackValueError:
			self.packetizeError("Bad encoding found in data; len={0}"
				.format(l))
		except UnpackTypeError as e:
			self.packetizeError("In data: {0}".format(e))
		return self.ERR

	#-------------------------------

	def packetizeData(self, msg):
		"""
		To be called wheneve new data arrives on the transport.
		This method will stuff the new data into the bufffer ring
		and then attempt to fetch as many messages as possible
		from the stream, stopping if either there's a wait condition
		or if an error occurred.
		"""
		self._ring.buffer(msg)
		go = self.OK
		while go is self.OK:
			if self._state is self.FRAME:
				go = self.__getFrame()
			else:
				go = self.__getPayload()

	#-------------------------------

	def packerizerReset(self):
		"""
		To be called on an error; flush out the packetizer and return it
		to its normal state.
		"""
		self._state = self.FRAME
		self._ring = Ring()

##=======================================================================
