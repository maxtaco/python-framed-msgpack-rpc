import re

def enum (**kwargs):
	return type('Enum', (), kwargs)

class InternetAddress (object):

    def __init__(self, tup=None, host=None, port=None, s=None, defhost="127.0.0.1"):
        self.host = host
        self.port = port
        if s:
            rxx = re.compile("(.*):(\d+)$")
            m = rxx.match(s)
            if m:
                self.host = m.group(1)
                self.port = int(m.group(2))
            else:
                self.host = s
        if tup:
            self.host = tup[0]
            self.port = tup[1]

        if not self.host or self.host is "-":
            self.host = defhost

    def __str__(self): 
        return "{0}:{1}".format(self.host, self.port)

    def __iter__(self): 
        return iter([self.host, self.port])


class OpenServerAddress (InternetAddress):
    def __init__(self, **kwargs):
        kwargs['defhost'] = "0.0.0.0"
        InternetAddress.__init__(self, **kwargs)

class ClosedServerAddress (InternetAddress):
    def __init__(self, **kwargs):
        kwargs['defhost'] = "127.0.0.1"
        InternetAddress.__init__(self, **kwargs)