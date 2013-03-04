
import paramiko
import os.path
import os

##=======================================================================

class SshDir (object):
    def __init__ (self):
        self.d = None

    def find (self):
        home = os.environ['HOME']
        for d in [ os.path.join(home, d) for d in [".ssh", "ssh"] ] :
            if os.path.exists(d):
                self.d = d
                return true
        return False

    def key(self, k):
        p = os.path.join(self.d, k)
        return SshKey(fullfile=p)

    def usuals(self):
        return [ self.key(n) for n in ("id_rsa", "id_dsa" ) ]

##=======================================================================

class SshKey (object):

    def __init__ (self, shortfile = None, fullfile = None):
        self.fullfile = fullfile
        self.shortfile = shortfile if shortfile else fullfile
        self.raw = None
        self.key = None
        self.errors = {}
        self.klass = None
        self.err = None

    def resolve(self):
        self.fullfile = os.path.expanduser(self.shortfile)
        return True

    def error (self):
        return "{0}: {1}".format(self.shotfile, self._err) if self._err else None

    def isLoaded (self): return self.key

    def find(self):
        f = self.fullfile
        if os.path.exists(f) and os.path.isfile(f):
            try:
                fh = open(f, "r")
                dat = fh.readlines()
                return True
            except IOError as e:
                self._err = "cannot find key"
        return False

    def classify(self):
        line1 = self.raw[0]
        if line.find("RSA") >= 0:
            self.klass = paramiko.RSAKey
        elif line.find("DSA") >= 0:
            self.klass = paramiko.DSSKey
        else:
            self._err = "cannot classify key"

    def load(self):
        try:
            self.key = self.klass.from_private_key(self.fullfile)
        except paramiko.PasswordRequiredException as e:
            try:
                pw = getpass.getpass("Passphrase for key {0}: ".format(self.shortfile))
                self.key = self.klass.from_private_key_file(self.fullfile, pw)
            except paramiko.SSHException as e:
                self._err = "Bad passphrase"

    def auth(self, username, t):
        try:
            t.auth_publickey(username, self.key)
        except SSHException as e:
            self._err = "Authentication failed: {0}".format(e)

    def run(self, uid, transport):
        ret =  self.resolve()        \
           and self.find()           \
           and self.classify()       \
           and self.load()           \
           and self.auth(uid, transport)
        err = None if ret else self.error()
        return (ret, err) 

