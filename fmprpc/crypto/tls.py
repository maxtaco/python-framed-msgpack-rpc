
import fmprpc.transport as transport
from tlslite import TLSConnection
from tlslite.errors import TLSRemoteAlert

##=======================================================================

class TlsStreamWrapper (transport.ClearStreamWrapper):

    def __init__(self, s, t):
        transport.ClearStreamWrapper.__init__(self, s, t)
        self._tls_transport = None

    # Read and write to the TLS stream, and not the underlying TCP socket
    def stream (self):
        return self._tls_transport

##=======================================================================

class TlsClientStreamWrapper (TlsStreamWrapper):

    def __init__ (self, s, transport):
        TlsStreamWrapper.__init__ (self, s, transport)

    def start (self):
        """Run a TLS handshake, and if that works, start up the constant reader...."""
        tc = TLSConnection(self._socket)
        p = self.transport()
        ret = False
        if p:
            u = p.uid()
            # Catch some some or exception here....
            p.info("Calling handshake for {0}".format(u))
            try:
                tc.handshakeClientSRP(u, p.pw(), p.previousTlsSession())
                self._tls_transport = tc
                p.setTlsSession(tc.session)
                # Call up to superclass....
                transport.ClearStreamWrapper.start(self)
                p.info("Handshake succeeded for #{0}".format(u))
                ret = True
            except TLSRemoteAlert as e:
                p.warn("SRP authentication error for {0}: {1}".format(u, e))
        else:
            p.warn("Transport was dead in TlsClientStreamWrapper.start")
        return ret

##=======================================================================

class TlsServerStreamWrapper (TlsStreamWrapper):
    def __init__ (self, s, transport):
        TlsStreamWrapper.__init__ (self, s, transport)

    def start (self):
        tc = TLSConnection(self._socket)
        p = self.transport()
        if p: p = p._parent
        if p and p.tlsDoHandshake(tc):
            self._tls_transport = tc
            transport.ClearStreamWrapper.start(self)
            ret = True 
        else:
            ret = False
        return ret

##=======================================================================

class TlsClientTransport (transport.Transport):

    def __init__ (self, **kwargs):
        self._pw = kwargs.pop('pw')
        self._uid = kwargs.pop('uid')
        transport.Transport.__init__(self, **kwargs)
        self._tls_session = None
        self.setWrapperClass(TlsClientStreamWrapper)

    def pw (self): return self._pw
    def uid (self): return self._uid
    def setTlsSession (self, s): self._tls_session = s
    def previousTlsSession (self): return self._tls_session

##=======================================================================

class TlsServerTransport (transport.Transport):
    def __init__ (self, **kwargs):
        transport.Transport.__init__(self, **kwargs)
        self.setWrapperClass(TlsServerStreamWrapper)

##=======================================================================

def enableServer(obj):
    """Call this function on an fmprpc.Server object to enable/require
    TLS on all incoming connections on this server.
    """
    if not hasattr(obj, "tlsDoHandshake"):
        raise NotImplementedError, "Server doesn't implement tlsDoHandshake"

    # All we really need to do is to change which wrapper class wraps
    # incoming connections,
    obj.setTransportClass(TlsServerTransport)

