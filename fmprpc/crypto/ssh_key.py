
import paramiko
import os.path
import os
import re
import base64
import getpass
import binascii

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

    def resolve(self):
        ff = os.path.expanduser(self.shortfile)
        if len(ff) and ff[0] != os.path.sep:
            ff = os.path.join(os.getcwd(), ff)
        self.fullfile = os.path.normpath(ff)
        return True

    def error (self):
        return "{0}: {1}".format(self.shortfile, self._err) if self._err else None

    def isLoaded (self): return self.key

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
            ret = self.loadFromTriple(*tuple(parts))
        else:
            self._err = "keyfile was in wrong format (expected 3 " + \
                " fields, space-delimited)"
        return ret

    def exportToTriple(self, enc = None):
        k = str(self.key)
        if enc: k = k.encode(enc)
        return (self.type, k, self.name)

    def exportToDict(self, enc = None):
        k = str(self.key)
        if enc: k = k.encode(enc)
        trip = self.exportToTriple(enc)
        f = self.fingerprint()
        return { "type" : trip[0], "key" : trip[1], "name" : trip[2], "fingerprint" : f }

    def loadFromDict(self, d):
        self.loadFromTriple(d.get("type"), d.get("key"), d.get("name"))

    @classmethod
    def createFromDict(klass, d):
        ret = SshPubkey()
        ret.loadFromDict(d)
        return ret

    def loadFromTriple(self, typ, data, name):
        klass = None
        ret = False
        self.type = typ
        self.name = name

        klass = self.keytype_to_klass(typ)

        if klass:
            try:
                data = base64.decodestring(data)
                self.key = klass(data=data)
                ret = True
            except binascii.error as e:
                self._err = "encoding error: {0}".format(e)
            except paramiko.SSHException as e:
                self._err = "invalid key: {0}".format(e)
        else:
            self._err = "Unknown key type: {0}".format(typ)

        return ret

    def fingerprint (self):
        return binascii.hexlify(self.key.get_fingerprint())

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
        ret = self.resolve()  \
          and self.find()     \
          and self.classify() \
          and self.load()
        if ret and uid and transport:
            ret = self.auth(uid, transport)
        err = None if ret else self.error()
        return (ret, err) 

