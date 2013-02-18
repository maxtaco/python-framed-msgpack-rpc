
##=======================================================================

import log

##=======================================================================

class Flags:
	NONE = 0x0
	METHOD = 0x1
	REMOTE = 0x2
	SEQID = 0x4
	TIMESTAMP = 0x8
	ERR = 0x10
	ARG = 0x20
	RES = 0x40
	TYPE = 0x80
	DIR = 0x100
	PORT = 0x200
	VERBOSE = 0x400
	ALL = 0xffffffff

	LEVEL_0 = NONE
	LEVEL_1 = METHOD | TYPE | DIR 
	LEVEL_2 = LEVEL_1 | SEQID | TIMESTAMP | REMOTE | PORT
	LEVEL_3 = LEVEL_2 | ERR
	LEVEL_4 = LEVEL_3 | RES | ARG

StringFlags = {
	"m" : Flags.METHOD,
	"a" : Flags.REMOTE,
	"s" : Flags.SEQID,
	"t" : Flags.TIMESTAMP,
	"e" : Flags.ERR,
	"p" : Flags.ARG,
	"r" : Flags.RES,
	"c" : Flags.TYPE,
	"d" : Flags.DIR,
	"v" : Flags.VERBOSE,
	"P" : Flags.PORT,
	"A" : Flags.ALL,
	"0" : Flags.LEVEL_0,
	"1" : Flags.LEVEL_1,
	"2" : Flags.LEVEL_2,
	"3" : Flags.LEVEL_3,
	"4" : Flags.LEVEL_4
}

class Direction:
	INCOMING : 1
	OUTGOING : 2

def flirDir(d):
	return (Direction.INCOMING if d is Direction.OUTGOING else Direction.OUTGOING)

class Type:
	SERVER : 1
	CLIENT_NOTIFY : 2
	CLIENT_CALL : 3

F2S = {
	Flags.DIR : {
		Direction.INCOMING : "in",
		Direction.OUTGOING : "out"
	},
	Flags.TYPE : {
		Type.SERVER : "server",
		Type.CLIENT_CALL : "client.invoke",
		Type.CLIENT_NOTIFY : "client.notify"
	}
}

def sflagsToFlags (s):
	s = "{0}".format(s)
	res = 0
	for ch in s:
		res |= StringFlags[ch]
	return res
