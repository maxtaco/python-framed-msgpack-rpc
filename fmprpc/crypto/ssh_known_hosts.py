
import os.path
import paramiko
import fmprpc.log as log

class KnownHostsRegistry (log.Base):

    def __init__ (self):
        log.Base.__init__(self, log.newDefaultLogger(prefix="SshKnownHosts"))
        self.hosts = {}

    def load (self):
        self.__loadOne(".ssh")
        self.__loadOne("ssh")
        if len(self.hosts) is 0:
            self.warn("Could not load any known SSH hosts; failure ahead")
        return self

    def __loadOne(self, dir):
        f = os.path.expanduser(os.path.join("~", dir, "known_hosts"))
        if os.path.exists(f) and os.path.isfile(f):
            tmp = paramiko.util.load_host_keys(f) 
            self.hosts.update(tmp)

    def verify (self, hostname, theirs):
        ok = False
        err = None
        row = self.hosts.get(hostname)
        if row:
            # get_name() return the type of key, like 'ssh-rsa'
            # or 'ssh-dsa', etc...
            typ = theirs.get_name()
            ours = row.get(typ)
        if not row:
            err = "No keys found for hostname {h}"
        elif not ours:
            err = "No key of type {t} found for hostname {h}"
        elif ours != theirs:
            err = "Wrong {t} key found for hostname {h}: key changed!"
        else:
            ok = True 
        if err: 
            err = err.format(h=hostname, t=typ)
            self.warn(err)
        return (ok,err)

_s = None
def singleton():
    global _s
    if not _s: _s = KnownHostsRegistry().load()
    return _s

def verify(hostname, theirs):
    return singleton().verify(hostname, theirs)



