
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
            tc.close()
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
        if kwargs.hash_key("known_hosts"):
            self.khr  = kwargs.pop("known_hosts")
        else:
            self.khr = skh.singleton()
        tranport.Transport.__init__ (self, **kwargs)
        self._ssh_session = None
        self.setWrapperClass(SshClientStreamWrapper)

    ##-----------------------------------------

    def __doAgentAuth(self, t):
        agent = paramiko.Agent()
        keys = agent.get_keys()
        for key in agent_keys:
            fp = hexlify(key.get_fingerprint())
            self.info("Trying ssh-agent key {0}".format(fp))
            try:
                t.auth_publickey(self.uid, k)
                self.info("Key succeeded: {0}".format(fp))
                return True
            except paramiko.SSHException:
                self.info("Key failed ({0})".format(fp))
        return False

    ##-----------------------------------------

    def __tryUsualKeys(self, t):
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
        return (ret, err)

    ##-----------------------------------------

    def __tryKey(self, k):
        kobj = ssh_key.SshPrivkey(shortfile=k)
        return kobj.run(uid = self.uid, transport = t)

    ##-----------------------------------------

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
        (ok, err) = self.khr.verify(self.remote().host, key)
        if not ok:
            self.reportError("hostAuth", err)
            return False

        # Now try the various attempts at client auth:
        #  1. First try the supplied key
        #  2. Then try the user's agent.
        #  3. Finally, try the default keys....
        if self.key:
            (ok, err) = self.__tryKey(t, self.key)
            if not ok:
                self.warn("Failed to authenticate with key {0}".format(self.key))
        elif not self.__doAgentAuth(t):
            (ok, err) = self.__tryUsualKeys(t)

        if not ok:
            self.warn("In client authentication: {0}".format(err))
            self.reportError("clientAuth", err)
        else:
            self._ssh_transport = t
            transport.Transport.start(self)
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
        if _parent:
            # Call up to tohe subclassed Server class for this...
            self._parent.sshAddServerKey(ssht)
            w = self._parent.sshGetAcceptTimeout()
            try:
                ssht.start_server(server = self)
                chan = ssht.accept(self._parent.sshGetAcceptTimeout())
                if chan is None:
                    self.warn("Failed to get an SSH channel")
            except paramiko.SSHException as e:
                self.warn("SSH negotiation failed: #{0}".format(e))
        else:
            self.warn("Dead parent in doSshHandshake()")
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
