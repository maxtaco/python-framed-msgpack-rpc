
import fmprpc.transport as transport
import fmprpc.crypto.ssh_known_hosts as skh
import fmprpc.crypto.ssh_key as ssh_key
import paramiko
import base64
from binascii import hexlify
import os
import socket
import os.path
from fmprpc.err import ServerKeyError
from fmprpc.util import safepop, formatFingerprint

##=======================================================================

class SshStreamWrapper (transport.ClearStreamWrapper):

    def __init__(self, s, t):
        transport.ClearStreamWrapper.__init__(self, s, t)
        self._ssh_transport = None
        self._ssh_channel = None
    def stream (self):
        return self._ssh_channel
    def shutdownStream (self, x, force):
        try:
            if self._ssh_channel:
                self._ssh_channel.close()
        except EOFError as e:
            self.info("EOFError in closing SSH channel")
        try:
            if self._ssh_transport:
                self._ssh_transport.close()
        except EOFError as e:
            self.info("EOFError in closing SSH transport")

##=======================================================================

class SshClientStreamWrapper(SshStreamWrapper):

    def __init__ (self, s, transport, uid = None, key = None, known_hosts = None):
        self.uid = uid
        self.key = key
        self.khr = known_hosts if known_hosts else khr.singleton()
        SshStreamWrapper.__init__ (self, s, transport)

    ##-----------------------------------------

    def start (self):
        """Run an SSH handshake, and if that works, start up a 
        constant reader thread."""
        self.debug("+ SshClientStreamWrapper")
        tc = paramiko.Transport(self._socket)
        p = self.transport()
        ret = False
        if not p:
            self.warn("Transport was dead in SshClientStreamWrapper")
        elif self.doSshHandshake(tc):
            self._ssh_transport = tc
            try:
                self._ssh_channel = tc.open_session()
                transport.ClearStreamWrapper.start(self)
                ret = True
            except paramiko.ChannelException as e:
                self.reportError("session", str(e))
                self.warn("Failed to open a session: {0}".format(e))
                ret = False
        else:
            self.warn("+ SshClientStreamWrapper closing due to bad handshake")
            tc.close()
        self.debug("- SshClientStreamWrapper")
        return ret

    ##-----------------------------------------

    def __doAgentAuth(self, t):
        self.debug("+ __doAgentAuth")
        agent = paramiko.Agent()
        keys = agent.get_keys()
        ret = False
        for key in keys:
            fp = hexlify(key.get_fingerprint())
            self.info("Trying ssh-agent key {0} {1}".format(fp, key.get_base64()))
            try:
                t.auth_publickey(self.uid, key)
                self.info("Key succeeded: {0}".format(fp))
                ret = True
                break
            except paramiko.SSHException as e:
                self.info("Key failed ({0}): {1}".format(fp, e))
        self.debug("- __doAgentAuth -> {0}".format(ret))
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
        self.info("+ __tryKey {0}".format(self.uid))
        (ok, err) = key.run(uid = self.uid, transport = transport)
        self.info("- __tryKey -> {0} for {1}: {2}".format(
            ok, key.fingerprint(), err))
        return (ok, err)

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

        self.info("- doSshHandshake -> {0}".format(ret))
        return ret

    ##-----------------------------------------

    def __negotiateChannel(self, t):
        ret = False
        self.info("+ __negotiateChannel")
        try:
            t.start_client()
            ret = True
        except paramiko.SSHException as e:
            msg = "SSH channel negotiation failed: {0}".format(e)
            self.info(msg)
            self.reportError('negotation', msg)
        self.debug ("- __negotiateChannel -> {0}".format(ret))
        return ret

    ##-----------------------------------------

    def reportError(self, t, e):
        p = self.transport()
        if p: p.reportError(t, e)

    ##-----------------------------------------

    def __doHostAuth(self, t):
        self.info ("+ __doHostAuth")
        key = t.get_remote_server_key()
        host = self.remote().host
        self.info("Got remote server key for {0}: {1}".format(host, key.get_base64()))
        self.debug("++ verify via known_hosts: {0}".format(self.khr))
        (ok, err) = self.khr.verify(host, key)
        if not ok:
            self.reportError("hostAuth", err)
            self.warn("Failed to verify host {0}: {1}".format(host, err))
        self.debug("+ __doHostAuth -> {0}".format(ok))
        return ok

    ##-----------------------------------------

    def __doClientAuth(self, t):

        if not self.uid:
            self.info("+ __doClientAuth -- anon -- skipped")
            return True

        # Now try the various attempts at client auth:
        #  1. First try the supplied key
        #  2. Then try the user's agent.
        #  3. Finally, try the default keys....
        self.info("+ __doClientAuth")
        if self.key:
            (ok, err) = self.__tryKey(key=self.key, transport=t)
            if not ok:
                self.warn("Failed to authenticate with key {0}".format(self.key))
        elif self.__doAgentAuth(t):
            ok = True
        else:
            (ok, err) = self.__tryUsualKeys(t)

        if not ok:
            self.warn("In client authentication: {0}".format(err))
            self.reportError("clientAuth", err)
            
        self.info("- __doClientAuth -> {0}".format(ok))
        return ok

    ##-----------------------------------------

    @classmethod
    def popArgs(klass, kwargs):
        ret = {}
        for i in ("uid", "key", "known_hosts"):
            ret[i] = safepop(kwargs, i)
        return ret

##=======================================================================

class SshClientTransport(transport.Transport): 

    def __init__ (self, **kwargs):
        self.__xwa = SshClientStreamWrapper.popArgs(kwargs)
        super(SshClientTransport, self).__init__(**kwargs)
        self.setWrapperClass(SshClientStreamWrapper)

    ##-----------------------------------------

    def extraWrapperArgs(self): return self.__xwa

##=======================================================================

class SshClientRobustTransport(transport.RobustTransport): 

    def __init__ (self, **kwargs):
        self.__xwa = SshClientStreamWrapper.popArgs(kwargs)
        super(SshClientRobustTransport, self).__init__(**kwargs)
        self.setWrapperClass(SshClientStreamWrapper)

    ##-----------------------------------------

    def extraWrapperArgs(self): return self.__xwa

    ##-----------------------------------------

##=======================================================================

class SshServerStreamWrapper (SshStreamWrapper):
    def __init__(self, s, transport):
        SshStreamWrapper.__init__ (self, s, transport)
        self._ssh_transport = None
        self._ssh_channel = None

    def is_authenticated(self):
        ret = False
        if self._ssh_transport:
            ret = self._ssh_transport.is_authenticated()
        return ret

    def get_username(self):
        ret = None
        if self._ssh_transport:
            ret = self._ssh_transport.get_username()
        return ret

    def start(self):
        self._ssh_transport = ssht = paramiko.Transport(self._socket)
        chan = None
        p = self.transport()
        if p: chan = p.doSshHandshake(ssht)
        if chan:
            self._ssh_channel = chan
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

    def is_authenticated(self):
        ret = False
        if self._stream_w:
            ret = self._stream_w.is_authenticated()
        return ret

    def get_username(self):
        ret = None
        if self._stream_w:
            ret = self._stream_w.get_username()
        return ret

    def check_channel_request(self, kind, chanid):
        ok = False
        if kind == 'session':

            # This is a good time to tell our subclass that the user is
            # authenticated, or to deny a session if the user's auth
            # is required...
            ok = self._parent.sshSessionRequest(self, chanid)
            if not ok:
                self.warn("Session request rejected for {0} (auth={1})".format(
                    self.get_username(), self.is_authenticated()))
        else:
            self.warn("Reject open request for channel {0}/{1}"
                .format(kind, chanid))
            ok = False

        if ok: ret = paramiko.OPEN_SUCCEEDED
        else : ret = paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED
        return ret

    def check_auth_password(self, username, pw):
        self.warn("Rejecting PW login attempt by {0} w/ {1}"
                .format(username, pw))
        return paramiko.AUTH_FAILED

    def check_auth_none(self, username):
        self.warn("Rejecting 'non' attempt by {0}".format(username))
        return paramiko.AUTH_FAILED

    def get_allowed_auths(self, username): return 'publickey'

    def check_channel_shell_request(self, channel): return False

    def check_channel_pty_request(self, channel, term, width, height, pixelwidth,
                                  pixelheight, modes):
        return False

    def check_auth_publickey(self, username, key):
        ok = False
        fp = formatFingerprint(key.get_fingerprint())
        self.info("+ check_auth_publickey {0}@{1}".format(username, fp))
        if self._parent:
            ok = self._parent.sshCheckAuthPublickey(username, key, self)
            self.info("- check_auth_publickey {0}@{1} -> {2}".format(username, fp, ok))
        else:
            self.warn("Auth pubkey failed due to dead parent for {0}".format(username))
        return (paramiko.AUTH_SUCCESSFUL if ok else paramiko.AUTH_FAILED)

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

class AnonSshServerTransport (SshServerTransport):
    """
    An SSH transport in which the user is anonymous.
    Useful for registering a new users, and maybe backend
    communication.
    """

    def __init__ (self, **kwargs):
        SshServerTransport.__init__(self, **kwargs)

    def get_allowed_auths(self, username): 
        return "none"

    def check_auth_none (self, username):
        return paramiko.AUTH_SUCCESSFUL

    def check_auth_publickey(self, username, key):
        return paramiko.AUTH_FAILED

    def check_channel_request(self, kind, chanid):
        return paramiko.OPEN_SUCCEEDED

##=======================================================================

def enableServer(obj):
    """Call this function on an fmprpc.Server object to enable/require
    SSH on all incoming connections on this server.
    """

    methods = [ "sshAddServerKey", "sshGetAcceptTimeout", "sshCheckAuthPublickey",
                "sshSessionRequest"]
    for m in methods:
        if not hasattr(obj, m):
            raise NotImplementedError, "Server doesn't implement {0}".format(m)

    obj.setTransportClass(SshServerTransport)

##=======================================================================

def enableAnonServer(obj):
    """Call this function on an fmprpc.Server object to enable/require
    SSH on all incoming connections on this server, but for 
    **anonymous** connections
    """

    methods = [ "sshAddServerKey", "sshGetAcceptTimeout" ]
    for m in methods:
        if not hasattr(obj, m):
            raise NotImplementedError, "Server doesn't implement {0}".format(m)

    obj.setTransportClass(AnonSshServerTransport)

##=======================================================================

class ServerBase (object):

    def __init__ (self, anon = False):
        self.anon = anon
        if anon: enableAnonServer(self)
        else:    enableServer(self)
        self._keys = []

    def readKey(self, fn, typ):
        if typ in ('ssh-rsa', 'rsa')   : return self.readRsaKey(fn)
        elif typ in ('ssh-dsa', 'dsa') : return self.readDsaKey(fn)
        else: raise ServerKeyError("no known key type {0}".format(typ))

    def readRsaKey (self, fn):
        ret = False
        try: 
            self._keys.append(paramiko.RSAKey(filename=fn))
            ret = True
        except IOError as e:
            pass
        return ret

    def readDsaKey (self, fn):
        ret = False
        try: 
            self._keys.append(paramiko.DSSKey(filename=fn))
            ret = True
        except IOError as e:
            pass
        return ret

    def sshGetAcceptTimeout(self): return 60

    def sshAddServerKey(self, ssht): 
        for k in self._keys:
            ssht.add_server_key(k)

    def firstKey (self): return self._keys[0]


##=======================================================================
