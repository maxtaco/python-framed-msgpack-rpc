
import fmprpc.transport as transport
import fmprpc.crypto.ssh_known_hosts as skh
import fmprpc.crypto.ssh_key as ssh_key
import paramiko
import base64
from binascii import hexlify
import os
import socket
import os.path

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
        self.debug("+ SshClientStreamWrapper")
        tc = paramiko.Transport(self._socket)
        p = self.transport()
        ret = False
        if not p:
            self.warn("Transport was dead in SshClientStreamWrapper")
        elif p.doSshHandshake(tc):
            self._ssh_transport = tc
            transport.ClearStreamWrapper.start(self)
            ret = True
        else:
            self.warn("+ SshClientStreamWrapper closing due to bad handshake")
            tc.close()
        self.debug("- SsshClientStreamWrapper")
        return ret

##=======================================================================

class SshClientTransport (transport.Transport):
    def __init__ (self, **kwargs):
        self.uid = kwargs.pop("uid")
        if kwargs.has_key("key"):
            # A loaded in paramaiko Key (decrypted, etc..)
            self.key = kwargs.pop("key")
        else:
            self.key = None
        if kwargs.has_key("known_hosts"):
            self.khr = kwargs.pop("known_hosts")
        else:
            self.khr = skh.singleton()
        transport.Transport.__init__ (self, **kwargs)
        self._ssh_session = None
        self.setWrapperClass(SshClientStreamWrapper)

    ##-----------------------------------------

    def __doAgentAuth(self, t):
        self.debug("+ __doAgentAuth")
        agent = paramiko.Agent()
        keys = agent.get_keys()
        ret = False
        for key in agent_keys:
            fp = hexlify(key.get_fingerprint())
            self.info("Trying ssh-agent key {0}".format(fp))
            try:
                t.auth_publickey(self.uid, k)
                self.info("Key succeeded: {0}".format(fp))
                ret = True
                break
            except paramiko.SSHException:
                self.info("Key failed ({0})".format(fp))
        self.debug("+ __doAgentAuth -> {0}".format(ret))
        return ret

    ##-----------------------------------------

    def __tryUsualKeys(self, t):
        self.debug("+ __tryUsualKeys")
        d = ssh_key.Dir()
        ret = False
        err = None
        if not d.find():
            err = "no SSH directory found in home"
        else:
            for kobj in d.usuals():
                (ret, err) = kobj.run(uid = self.uid, transport = t)
                if ret:
                    break
        self.debug("- __tryUsualKeys -> {0}".format(ret))
        return (ret, err)

    ##-----------------------------------------

    def __tryKey(self, transport, key):
        self.debug("+ __tryKey '{0}'".format(key))
        kobj = ssh_key.SshPrivkey(shortfile=key)
        ret = kobj.run(uid = self.uid, transport = transport)
        self.debug("+ __tryKey -> {0}".format(ret))
        return ret

    ##-----------------------------------------

    def doSshHandshake(self, t):
        """Given the SSH Transport object t, do the handhake.
        The two main parts of this are: (1) verifying the server's
        public key; and (2) authenticating the client to the
        user."""

        self.info("+ doSshHandshake")

        ret = self.__negotiateChannel(t) and \
              self.__doHostAuth(t) and \
              self.__doClientAuth(t)

        if ret:
            self._ssh_transport = t
            transport.Transport.start(self)

        self.info("- doSshHandshake / status={0}".format(ret))
        return ret

    ##-----------------------------------------

    def __negotiateChannel(self, t):
        self.info("+ __negotiateChannel")
        try:
            t.start_client()
            ret = True
        except paramiko.SSHException as e:
            msg = "SSH channel negotiation failed: #{e}".format(e)
            self.info(msg)
            self.reportError('negotation', msg)
        self.debug ("- __negotiateChannel -> {0}".format(ret))
        return ret

    ##-----------------------------------------

    def __doHostAuth(self, t):
        self.info ("+ __doHostAuth")
        key = t.get_remote_server_key()
        self.info("Got remote server key: {0}".format(key.get_base64()))
        self.debug("++ verify key via known_hosts: {0}".format(self.khr))
        (ok, err) = self.khr.verify(self._remote.host, key)
        if not ok:
            self.reportError("hostAuth", err)
            self.warn("Failed to verify host {0}: {1}".format(err))
        self.debug("+ __doHostAuth -> {0}".format(ok))
        return ok

    ##-----------------------------------------

    def __doClientAuth(self, t):

        # Now try the various attempts at client auth:
        #  1. First try the supplied key
        #  2. Then try the user's agent.
        #  3. Finally, try the default keys....
        self.info("+ __doClientAuth")
        if self.key:
            (ok, err) = self.__tryKey(key=self.key, transport=t)
            if not ok:
                self.warn("Failed to authenticate with key {0}".format(self.key))
        elif not self.__doAgentAuth(t):
                (ok, err) = self.__tryUsualKeys(t)

        if not ok:
            self.warn("In client authentication: {0}".format(err))
            self.reportError("clientAuth", err)
            
        self.info("- __doClientAuth -> {0}".format(ok))
        return ok

##=======================================================================

class SshServerStreamWrapper (SshStreamWrapper):
    def __init__(self, s, transport):
        SshStreamWrapper.__init__ (self, s, transport)

    def start(self):
        ssht = paramiko.Transport(self._socket)
        chan = None
        p = self.transport()
        if p: chan = p.doSshHandshake(ssht)
        if chan:
            self._ssh_transport = chan
            transport.ClearStreamWrapper.start(self)
            ret = True 
        else:
            ret = False
        return ret

##=======================================================================

class SshServerTransport (transport.Transport, paramiko.ServerInterface):
    def __init__(self, **kwargs):
        transport.Transport.__init__ (self, **kwargs)
        self.setWrapperClass(SshServerStreamWrapper)

    def check_channel_request(self, kind, chanid):
        if kind == 'session':
            ret = paramiko.OPEN_SUCCEEDED
        else:
            self.warn("Reject open request for channel {0}/{1}"
                .format(kind, chanid))
            ret = paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED
        return ret

    def check_auth_password(self, username, pw):
        self.warn("Rejecting PW login attempt by {0} w/ {1}"
                .format(username, pw))
        return paramiko.AUTH_FAILED

    def get_allowed_auths(self, username): return 'publickey'

    def check_channel_shell_request(self, channel): return False

    def check_channel_pty_request(self, channel, term, width, height, pixelwidth,
                                  pixelheight, modes):
        return False

    def check_auth_publickey(self, username, password):
        if self._parent:
            ret = self._parent.sshCheckAuthPublickey(username, key)
        else:
            self.warn("Auth pubkey failed due to dead parent for {0}".format(username))
            ret = paramiko.AUTH_FAILED
        return ret

    def doSshHandshake(self, ssht):
        chan = None
        p = self._parent
        self.debug("+ SshServerTransport::doSshHandshake")
        if p:
            # Call up to the subclassed Server class for this...
            p.sshAddServerKey(ssht)
            w = p.sshGetAcceptTimeout()
            try:
                self.debug("++ SSH start_server")
                ssht.start_server(server = self)
                self.debug("++ SSH Accept")
                chan = ssht.accept(p.sshGetAcceptTimeout())
                self.debug("-- SSH Accept")
                if chan is None:
                    self.warn("Failed to get an SSH channel")
            except paramiko.SSHException as e:
                self.warn("SSH negotiation failed: #{0}".format(e))
        else:
            self.warn("Dead parent in doSshHandshake()")
        self.debug("- SshServerTransport::doSshHandshake")
        return chan

##=======================================================================

def enableServer(obj):
    """Call this function on an fmprpc.Server object to enable/require
    SSH on all incoming connections on this server.
    """

    methods = [ "sshAddServerKey", "sshGetAcceptTimeout", "sshCheckAuthPublickey"]
    for m in methods:
        if not hasattr(obj, m):
            raise NotImplementedError, "Server doesn't implement {0}".format(m)

    obj.setTransportClass(SshServerTransport)

##=======================================================================

class ServerBase (object):

    def __init__ (self):
        self._key = None
        enableServer(self)

    def readRsaKey (self, fn):
        ret = False
        try: 
            self._key = paramiko.RSAKey(filename=fn)
            ret = True
        except IOError as e:
            pass
        return ret

    def readDsaKey (self, fn):
        ret = False
        try: 
            self._key = paramiko.RSAKey(filename=fn)
            ret = True
        except IOError as e:
            pass
        return ret

    def sshGetAcceptTimeout(self): return 60
    def sshAddServerKey(self, ssht): ssht.add_server_key(self._key)
