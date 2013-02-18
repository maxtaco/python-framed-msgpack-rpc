

class Levels:
	NONE = 0 
	DEBUG = 1
	INFO = 2
	WARN = 3
	ERROR = 4
	FATAL = 5
	TOP = 6

	DEFAULT = INFO

class Logger:

	def __init__ (self, prefix="RPC", remote="-", level=None):
		self.prefix = prefix 
		self.remote = remote
		self.level = level if level else Levels.DEFAULT

print Levels.DEFAULT