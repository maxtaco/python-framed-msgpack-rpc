
import fmprpc.transport as transport
import fmprpc.crypto.ssh_known_hosts as skh
import paramiko
import base64
from binascii import hexlify
import os
import socket

##=======================================================================

class SshStreamWrapper (transport.ClearStreamWrapper):

    def __init__(self, s, t):
        transport.ClearStreamWrapper.__init__(self, s, t)
        self._ssh_transport = None
    def stream (self):
        return self._ssh_transport
    def shutdownStream (self, x, force):
        if self._ssh_transport:
            self._ssh_transport.close()

##=======================================================================

class SshClientStreamWrapper(SshStreamWrapper):

    def __init__ (self, s, transport):
        SshStreamWrapper.__init__ (self, s, transport)

    def start (self):
        """Run an SSH handshake, and if that works, start up a 
        constant reader thread."""
        tc = paramiko.Transport(self._socket)
        p = self.transport()
        ret = False
        if not p:
            self.warn("Transport was dead in SshClientStreamWrapper")
        else if p.doSshHandshake(tc):
            self._ssh_transport = tc
            transport.ClearStreamWrapper.start(self)
            ret = True
        else:
            tc.close()
        return ret

##=======================================================================

class SshClientTransport (tranport.Transport):
    def __init__ (self, **kwargs):
        tranport.Transport.__init__ (self, **kwargs)
        self._ssh_session = None
        self.setWrapperClass(SshClientStreamWrapper)

    def doSshHandshake(self, t):
        """Given the SSH Transport object t, do the handhake.
        The two main parts of this are: (1) verifying the server's
        public key; and (2) authenticating the client to the
        user."""

        self.info("Calling SSH channel negotation")
        try:
            t.start_client()
        except paramiko.SSHException as e:
            msg = "SSH channel negotiation failed: #{e}".format(e)
            self.info(msg)
            self.reportError('negotation', msg)
            return False
        key = t.get_remote_server_key()
        (ok, err) = skh.verify(self.remote().host, key)
        if not ok:
            self.reportError("hostAuth", err)
            return False

