
import transport
from tlslite import TLSConnection

class TlsStreamWrapper (transport.ClearStreamWrapper):

    def __init__(self, s, transport):
        transport.ClearStreamWrapper.__init__(s, transport)
        self._tls_transport = None

    # Read and write to the TLS stream, and not the underlying TCP socket
    def stream (self):
        return self._tls_transport

    def start (self):
        """Run a TLS handshake, and if that works, start up the constant reader...."""
        tc = TLSConnection(self._socket)
        p = self.transport()
        if p:
            # Catch some some or exception here....
            tc.handshakeClientSRP(p.uid(), p.pw(), p.previousTlsSession())
            self._tls_transport = tc
            p.setTlsSession(tc.session)
            # Call up to superclass....
            transport.ClearStreamWrapper.start(self)
        else:
            raise err.DeadTransportError("transport was dead in TlsStream start")

class TlsTranport (transport.Transport):

    def __init__ (self, **kwargs):
        self._pw = kwargs.pop('pw')
        self._uid = kwargs.pop('uid')
        transport.Transport.__init__(self, **kwargs)
        self._tls_session = None
        self.WrapperClass = TlsStreamWrapper

    def pw (self): return self._pw
    def uid (self): return self._uid
    def setTlsSession (self, s): self._tls_session = s
    def prevoiusTlsSession (self): return self._tls_session

