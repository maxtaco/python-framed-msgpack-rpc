
import paramiko
import os.path
import os
import re
import base64
import getpass
import binascii
from fmprpc.util import formatFingerprint

##=======================================================================

class Dir (object):
    def __init__ (self):
        self.d = None

    def find (self):
        home = os.environ['HOME']
        for d in [ os.path.join(home, d) for d in [".ssh", "ssh"] ] :
            if os.path.exists(d):
                self.d = d
                return True
        return False

    def key(self, k, klass=None):
        p = os.path.join(self.d, k)
        if not klass:
            rxx = re.compile("\.pub$")
            if rxx.search(p):
                klass = SshPubkey
            else:
                klass = SshPrivkey
        return klass(fullfile=p)

    def usuals(self):
        return [ self.key(n) for n in ("id_rsa", "id_dsa" ) ]
    def usualsPublic (self):
        return [ self.key(n) for n in ("id_rsa.pub", "id_dsa.pub" ) ]

##=======================================================================

class Base (object):
    def __init__ (self, shortfile = None, fullfile = None):
        self.fullfile = fullfile
        self.shortfile = shortfile if shortfile else fullfile
        self.raw = None
        self._err = None
        self.key = None
        self.type = None
        self.name = None

    def resolve(self):
        ff = os.path.expanduser(self.shortfile)
        if len(ff) and ff[0] != os.path.sep:
            ff = os.path.join(os.getcwd(), ff)
        self.fullfile = os.path.normpath(ff)
        return True

    def error (self):
        return "{0}: {1}".format(self.shortfile, self._err) if self._err else None

    def isLoaded (self): return bool(self.key)

    def find(self):
        f = self.fullfile
        if os.path.exists(f) and os.path.isfile(f):
            try:
                fh = open(f, "r")
                self.raw = fh.readlines()
                return True
            except IOError as e:
                pass
        self._err = "cannot find key"
        return False

    def fingerprint (self, colons = False):
        return formatFingerprint(self.key.get_fingerprint(), colons)

##=======================================================================

class SshPubkey (Base):
    """
    A little wrapper around a paramiko public key, for things like
    reading the files out of the file system, and capturing a row
    of an SSH known_hosts file.
    """

    keytype_lookup = {
        "ssh-rsa": paramiko.RSAKey,
        "ssh-dsa" : paramiko.DSSKey
    }

    def __init__(self, shortfile = None, fullfile = None):
        Base.__init__ (self, shortfile = shortfile, fullfile = fullfile)

    @classmethod
    def keytype_to_klass(klass, typ):
        return klass.keytype_lookup.get(typ)

    def load(self):
        parts = self.raw[0].split()
        ret = False
        if len(parts) == 3:
            # We'll have a base64-encoded data, and not the raw data, but
            # this is compatible with the arg ordering of loadFromTuple()
            ret = self.loadFromTuple(*tuple(parts))
        else:
            self._err = "keyfile was in wrong format (expected 3 " + \
                " fields, space-delimited)"
        return ret

    def exportToTriple(self, enc = False):
        k = str(self.key)
        if enc: k = base64.b64encode(k)
        return (self.type, k, self.name)

    def exportToDict(self, enc = False):
        k = str(self.key)
        ret = { 
            "type"        : self.type,
            "name"        : self.name, 
            "fingerprint" : self.fingerprint(False)
        }
        if enc: ret["key"]  = base64.b64encode(k)
        else:   ret["rkey"] = k
        return ret

    def loadFromDict(self, d):
        self.loadFromTuple(d.get("type"), d.get("key"), d.get("name"), d.get("rkey"))

    @classmethod
    def getRawKey(klass, d): 
        raw = d.get("rkey")
        if raw is None:
            e = d.get("key")
            if e: raw = base64.b64decode(e)
        return raw

    @classmethod
    def createFromDict(klass, d):
        ret = SshPubkey()
        ret.loadFromDict(d)
        return ret

    @classmethod
    def createFromKey(klass, k):
        ret = SshPubkey()
        ret.type = k.get_name()
        ret.key = k 
        ret.name = "anon"
        return ret

    def loadFromTuple(self, typ, b64data = None, name = None, data = None):
        klass = None
        ret = False
        self.type = typ
        self.name = name

        klass = self.keytype_to_klass(typ)

        if not klass:
            self._err = "Unknown key type: {0}".format(typ)
        elif b64data:
            try:
                data = base64.b64decode(b64data)
            except TypeError as e:
                self._err = "encoding error: {0} (dat={1})".format(e, data)

        if data:
            try:
                self.key = klass(data=data)
                ret = True
            except paramiko.SSHException as e:
                self._err = "invalid key: {0}".format(e)

        return ret

    def run(self):
        ret = self.resolve() \
          and self.find()    \
          and self.load()           
        err = None if ret else self.error()
        return (ret, err) 

##=======================================================================

class SshPrivkey(Base):

    def __init__ (self, shortfile = None, fullfile = None):
        Base.__init__ (self, shortfile = shortfile, fullfile = fullfile)
        self.klass = None
        self._loaded = False

    def classify(self):
        line = self.raw[0]
        ok = True
        if line.find("RSA") >= 0:
            self.klass = paramiko.RSAKey
        elif line.find("DSA") >= 0:
            self.klass = paramiko.DSSKey
        else:
            ok = False
            self._err = "cannot classify key"
        return ok

    def load(self):
        try:
            self.key = self.klass.from_private_key_file(self.fullfile)
        except paramiko.PasswordRequiredException as e:
            try:
                pw = getpass.getpass("Passphrase for key {0}: ".format(self.shortfile))
                self.key = self.klass.from_private_key_file(self.fullfile, pw)
            except paramiko.SSHException as e:
                self._err = "Bad passphrase"
        return bool(self.key)

    def auth(self, username, t):
        ret = False
        try:
            t.auth_publickey(username, self.key)
            ret = True
        except paramiko.SSHException as e:
            self._err = str(e)
        return ret

    def run(self, uid = None, transport = None):
        if not self._loaded:
            ret = self.resolve()  \
              and self.find()     \
              and self.classify() \
              and self.load()
            self._loaded = ret
        else:
            ret = True 
        if ret and uid and transport:
            ret = self.auth(uid, transport)
        err = None if ret else self.error()
        return (ret, err) 

