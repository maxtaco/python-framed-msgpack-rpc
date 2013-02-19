"""
A centralized module for logging to different levels, and a mechanism
to set the level appropriately.
"""

import sys


##=======================================================================

class Levels:
	"""
	Enumerated class for log levels. 
	"""
	NONE = 0 
	DEBUG = 1
	INFO = 2
	WARN = 3
	ERROR = 4
	FATAL = 5
	TOP = 6

	DEFAULT = INFO

	@classmethod
	def setDefault(klass, l): 
		"""
		Set the default log level for all future Logger objects;
		this won't retoractively affect those that are already allocated.
		"""
		klass.DEFAULT = l

##=======================================================================

def trim(msg):
	"""
	Trim the given message, stripping out all trailing whitespace
	and adding a final newline in preparation for logging.
	"""
	i = len(msg) - 1
	while i >= 0 and msg[i].isspace():
		i -= 1
	i += 1
	return msg[0:i] + "\n"

##=======================================================================

class Logger (object):
	"""
	A logger class that exposes the 'debug', 'info', 'warn',
	'error', and 'fatal' methods for logging.
	"""

	def __init__ (self, prefix="RPC", remote="-", level=None):
		self.prefix = prefix 
		self.remote = remote
		self.level = level if level else Levels.DEFAULT
		self.outputHook = self.output

	def setLevel (self, l): self.level = l
	def setRemote (self, r): self.remote = r
	def setPrefix (self, p): self.prefix = p

	def debug(self, msg): self._log(msg, Levels.DEBUG, "D")
	def info (self, msg): self._log(msg, Levels.INFO , "I")
	def warn (self, msg): self._log(msg, Levels.WARN , "W")
	def error(self, msg): self._log(msg, Levels.ERROR, "E")
	def fatal(self, msg): self._log(msg, Levels.FATAL, "F")

	def _log (self, msg, level, display, ohook=None):
		if level >= self.level: 
			msg = trim(msg)
			parts = []
			if self.prefix: parts.append(self.prefix)
			if display    :       parts.append("[{0}]".format(display))
			if self.remote: parts.append(self.remote)
			if msg:         parts.append(msg)
			if not ohook:   ohook = self.outputHook
			ohook(" ".join(parts))

	def output(self, msg):
		sys.stderr.write(msg)

	def makeChild(self, **kwargs):
		return Logger(**kwargs)

##=======================================================================

_defaultLoggerClass = Logger
def setDefaultLoggerClass (k): _defaultLoggerClass = k
def newDefaultLogger (**kwargs): return _defaultLoggerClass(**kwargs)

##=======================================================================

x = Logger()
x.debug("hello")
