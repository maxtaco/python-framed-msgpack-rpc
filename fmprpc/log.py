"""
A centralized module for logging to different levels, and a mechanism
to set the level appropriately.
"""

import sys
from fmprpc.err import LogError

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

    _DEFAULT = INFO

    @classmethod
    def setDefault(klass, l): 
        """
        Set the default log level for all future Logger objects;
        this won't retroactively affect those that are already allocated.
        """

        # If you pass None, we assume no change...
        if l is None:
            return

        if type(l) is str:
            try:
                n = int(l)
            except ValueError as e:
                try:
                    n = getattr(klass, l.upper())
                except AttributeError as e:
                    raise LogError("unknown level {0}".format(l))
        else:
            n = l

        if n < 0 or n >= klass.TOP: 
            raise LogError("log level {0} is out of range".format(n))

        klass._DEFAULT = n

    @classmethod
    def getDefault(klass): return klass._DEFAULT

##=======================================================================

def trim(msg):
    """
    Trim the given message, stripping out all trailing whitespace
    and adding a final newline in preparation for logging.
    """
    msg = str(msg)
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

    def __init__ (self, prefix="RPC", remote=None, level=None):
        self.prefix = prefix 
        self.remote = remote
        self.level = (lambda : level) if level else Levels.getDefault
        self.outputHook = self.output

    def setLevel (self, l): self.level = (lambda : l)
    def setRemote (self, r): self.remote = r
    def setPrefix (self, p): self.prefix = p

    def debug(self, msg): self._log(msg, Levels.DEBUG, "D")
    def info (self, msg): self._log(msg, Levels.INFO , "I")
    def warn (self, msg): self._log(msg, Levels.WARN , "W")
    def error(self, msg): self._log(msg, Levels.ERROR, "E")
    def fatal(self, msg): self._log(msg, Levels.FATAL, "F")

    def _log (self, msg, level, display, ohook=None):
        if level >= self.level(): 
            msg = trim(msg)
            parts = []
            if self.prefix: parts.append(self.prefix)
            if display    : parts.append("[{0}]".format(display))
            if self.remote: parts.append(str(self.remote))
            if msg:         parts.append(msg)
            if not ohook:   ohook = self.outputHook
            ohook(" ".join(parts))

    def output(self, msg):
        sys.stderr.write(msg)

    def makeChild(self, remote=None, prefix=None):
        if not prefix: prefix = self.prefix
        if not remote: remote = self.remote
        return Logger(prefix=prefix, remote=remote, level = self.level)

##=======================================================================

class Base(object):
    def __init__ (self, log_obj = None):
        if log_obj:
            self.log_obj = log_obj
    def getLogger(self): return self.log_obj
    def setLogger(self, o): self.log_obj = o
    def warn(self, e) : self.log_obj.warn(e)
    def info(self, e) : self.log_obj.info(e)
    def fatal(self, e): self.log_obj.fatal(e)
    def debug(self, e): self.log_obj.debug(e)
    def error(self, e): self.log_obj.error(e)
  
##=======================================================================

_defaultLoggerClass = Logger
def setDefaultLoggerClass (k): _defaultLoggerClass = k
def newDefaultLogger (**kwargs): return _defaultLoggerClass(**kwargs)

##=======================================================================

