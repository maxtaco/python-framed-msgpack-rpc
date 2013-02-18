
import sys

class Levels:
	NONE = 0 
	DEBUG = 1
	INFO = 2
	WARN = 3
	ERROR = 4
	FATAL = 5
	TOP = 6

	DEFAULT = INFO

def trim(msg):
	i = len(msg) - 1
	while i >= 0 and msg[i].isspace():
		i -= 1
	i += 1
	if i is 0:
		ret = None
	else:
		ret = msg[0:i]
	return ret

class Logger:

	def __init__ (self, prefix="RPC", remote="-", level=None):
		self.prefix = prefix 
		self.remote = remote
		self.level = level if level else Levels.DEFAULT
		self.outputHook = self.output

	def set_level (self, l): self.level = l
	def set_remote (self, r): self.remote = r
	def set_prefix (self, p): self.prefix = p

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
		sys.stderr.write(msg + "\n")


_defaultLoggerClass = Logger

def setDefaultLevel (l): Levels.DEFAULT = l
def setDefaultLoggerClass (k): _defaultLoggerClass = k
def newDefaultLogger (**kwargs): return _defaultLoggerClass(**kwargs)



