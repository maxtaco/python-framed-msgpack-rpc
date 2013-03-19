
import os.path
import paramiko
import fmprpc.log as log
from hashlib import sha1
from hmac import HMAC

##=======================================================================

class KnownHostsRegistry (log.Base):
    """
    Read in the user's SSH known_hosts file, and perform lookups,
    on either plaintext hostnames, or those that have been obscured
    with known_hosts hashing (see here: http://nms.csail.mit.edu/projects/ssh/).

    I'm not crazy about this style of known host registry since it only 
    admits one ker per host.  It gives more freedom to allow a few
    different keys for a given host, especially during an upgrade.
    """

    def __init__ (self):
        log.Base.__init__(self, log.newDefaultLogger(prefix="SshKnownHosts"))
        self.hosts = {}
        self.hashes = []

    def __str__(self):
        return "KnownHostsRegistry: {0}".format(repr(self.hosts))

    def load (self):
        self.__loadOne(".ssh")
        self.__loadOne("ssh")
        if len(self.hosts) is 0:
            self.warn("Could not load any known SSH hosts; failure ahead")
        self.__findHashes()
        return self

    def __findHashes(self):
        for k in self.hosts.keys():
            self.__findHash(k)

    def __findHash(self, k):
        parts = k.split("|")
        if len(parts) is 4 and parts[1] is "1":
            v = [ p.decode('base64') for p in parts[2:4]]
            self.hashes.append(tuple([k] + v))

    def __loadOne(self, dir):
        f = os.path.expanduser(os.path.join("~", dir, "known_hosts"))
        if os.path.exists(f) and os.path.isfile(f):
            tmp = paramiko.util.load_host_keys(f) 
            self.hosts.update(tmp)

    def __findHashedHostname(self,hostname):
        for (key,salt,res) in self.hashes:
            hmac = HMAC(salt, None, sha1)
            hmac.update(hostname)
            ours = hmac.digest()
            if ours == res:
                return self.hosts.get(key)
        return None

    def lookup (self, hostname):
        """Find a row for the given hostname; either in plaintext
        or as  hashed hostname as per Jayeon's patch to ssh, which is
        standard on Linux but not on Mac."""
        row = self.hosts.get(hostname)
        if not row:
            row = self.__findHashedHostname(hostname)
        return row

    def add(self, host, type, key):
        self.hosts[host] = { type : key }
        self.__findHash(host)

    def verify (self, hostname, theirs):
        ok = False
        err = None
        typ = theirs.get_name()
        row = self.lookup(hostname)

        if row:
            # get_name() return the type of key, like 'ssh-rsa'
            # or 'ssh-dsa', etc...
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

def lookup(hostname):
    return singleton().lookup(hostname)

def verify(hostname, theirs):
    return singleton().verify(hostname, theirs)



