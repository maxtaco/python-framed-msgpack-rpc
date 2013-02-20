import socket
import log
import dispatch
import debug
import threading
import time
import ilist
import util
import weakref

##=======================================================================

class ConstantReader (threading.Thread):

    def __init__ (self, transport, wrapper):
        self.transport = transport
        self.wrapper = wrapper
        threading.Thread.__init__(self)

    def run (self):
        go = True
        while go and self.wrapper:
            op = None
            try:
                buf = self.wrapper.recv(0x1000)
                self.transport().info("Got data: {0}".format(util.formatRaw(buf)))
                if buf:
                    op = lambda : self.transport().packetizeData(buf)
                else:
                    op = lambda : self.transport().handleClose(self.wrapper)
                    go = False
            except IOError as e:
                op = lambda : self.transport().handleError(e, self.wrapper)
                go = False
            if op:
                self.transport().atomicOp(op)

##=======================================================================

class ClearStreamWrapper (object):
    """
    A shared wrapper around a socket, for which close() is idempotent. Of course,
    no encyrption on this interface.
    """
    def __init__ (self, s, transport, g):
        # Disable Nagle by default on all sockets...
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.socket = s
        self.generation = g
        self.write_closed_warn = False
        self._reader = None
        self.transport = weakref.ref(transport)
        if socket:
            self._remote = util.InternetAddress(tup=s.getpeername())

    def launchReader (self):
        if self._reader:
            self.transport().error("Refusing to launch a second reader...")
        else:
            self._reader = ConstantReader(self.transport, self)
            self._reader.start()

    def close (self):
        """
        Return True if we did the actual close, and false otherwise
        """
        ret = False
        if self.socket:
            ret = True
            x = self.socket
            self.socket = None
            self.transport().dispatchReset()
            self.transport().packetizerReset()
            x.close()
        return ret

    def write (self, msg):
        """
        Write the message to the socket, calling send(2) repeatedly
        until the buffer is flushed out.  Use low-level socket calls.
        """

        self.transport().info("writing data: {0}".format(util.formatRaw(msg)))
        if self.socket:
            self.socket.sendall(msg)
        elif not self.write_closed_warn:
            self.write_closed_warn = True
            self.transport().warn("write on closed socket")

    def recv(self, n):
        self.transport().info("recv {0}".format(n))
        if self.socket:
            return self.socket.recv(n)
        else:
            self.transport().warn("calling recv on a closed socket")
            return None

    def stream (self): return self.socket
    def isConnected (self): return not not self.socket
    def getGeneration (self): return self.generation

    def remote(self): return self._remote

##=======================================================================

class Transport (dispatch.Dispatch):

    def __init__ (self, remote=None, tcp_opts={}, 
                  stream=None, log_obj=None, parent=None,
                  hooks={}, dbgr=None):

        dispatch.Dispatch.__init__(self)

        # We don't store the TCP stream directly, but rather, sadly,
        # a **wrapper** around the TCP stream.  This is to make stream
        # closes idempotent. The case we want to avoid is two closes
        # on us (due to errors), but the second closing the reconnected
        # stream, rather than the original stream.  This level of
        # indirection solves this.
        self._stream_w = None

        self._remote = remote
        self._tcp_opts = tcp_opts
        self._explicit_close = False
        self._parent = parent

        self.setLogger(log_obj)
        self._generation = 1
        self._dbgr = dbgr
        self._hooks = hooks

        self._node = ilist.Node(self)

        # potentially set @_tcpw to be non-null
        if stream:
            self.__activateStream(stream)

    ##-----------------------------------------

    def serverListNode (self): return self._node

    ##-----------------------------------------

    def setDebugger (d): self._dbgr = d
  
    ##---------------------------------------

    def setDebugFlags (f):
        self.setDebugger(debug.makeDebugger(d,self._log_obj))
   
    ##-----------------------------------------

    def __nextGeneration (self):
        """To be called by StreamWrapper objects but not by
        average users."""
        ret = self._generation
        self._generation += 1
        return ret
 
    ##-----------------------------------------

    def getGeneration (self):
        return self._stream_w.getGeneration() if self._stream_w else -1 
 
    ##-----------------------------------------

    def remote (self):
        """
        Get the remote address of the person we're talking to.  It's better
        to grab it from the currently active stream wrapper, but on startup
        it might not be available, so we grab it from the object
        (if it exists).
        """
        return self._stream_w.remote() if self._stream_w else self._remote

    ##-----------------------------------------

    def setLogger (self, o):
        if not o:
            o = log.newDefaultLogger()
        self._log_obj = o
        self._log_obj.setRemote(self.remote())

    ##-----------------------------------------

    def getLogger (self): return self._log_obj

    ##-----------------------------------------

    def isConnected (self):
        if self._stream_w: return self._stream_w.isConnected()
        else:              return False

    ##-----------------------------------------

    def connect (self):
        print("connect... acquire lock...")
        self._lock.acquire()
        print("connect ... acquired lock....")
        if not self.isConnected():
            print("connect... critical section...")
            ret = self.__connectCriticalSection()
            print("connect...finishing critical section")
        else:
            ret = True
        self._lock.release()
        if not ret:
            print("shit dawg, neet to reconnect....")
            self.__reconnect(True)
        return ret

    ##-----------------------------------------

    def __reconnect(self, first):
        """
        The standard transport won't try to reconnect....
        """
        return False

    ##-----------------------------------------

    def reset(self, w):
        if not w: w = self._stream_w
        self.__close(w)

    ##-----------------------------------------

    def __del__(self):
        print("calling close...")
        self.close()
   
    ##-----------------------------------------

    def close (self):
        """
        Call to explicitly close this connection.  After an explicit close,
        reconnects are not attempted....
        """
        self._explicit_close = True
        if self._stream_w:
            w = self._stream_w
            self._stream_w = None
            w.close()
    #
    # /Public API
    ##---------------------------------------------------

    def warn(self, e) : self._log_obj.warn(e)
    def info(self, e) : self._log_obj.info(e)
    def fatal(self, e): self._log_obj.fatal(e)
    def debug(self, e): self._log_obj.debug(e)
    def error(self, e): self._log_obj.error(e)
  
    ##-----------------------------------------

    def __close (self, tcpw):
        # If an optional close hook was specified, call it here...
        if (self._hooks and self._hooks.eof):
            self._hooks.eof(tcpw)
        if tcpw.close():
            self.__reconnect(False)

    ##-----------------------------------------

    def atomicOp(self,op):
        self._lock.acquire()
        op()
        self._lock.release()
   
    ##-----------------------------------------

    def handleError(self, e, tcpw):
        self.error(e)
        self.__close(tcpw)

    ##-----------------------------------------
 
    def packetizeError (self, err): 
        # I think we'll always have the right TCP stream here
        # if we grab the one in the this object.  A packetizer
        # error will happen before any errors in the underlying
        # stream
        self.handleError("In packetizer: {0}".format(err), self._stream_w)
    
    ##-----------------------------------------

    def handleClose (self, tcpw):
        if not self._explicit_close:
            self.info("EOF on transport")
            self.__close(tcpw)

        # for TCP connections that are children of Listeners,
        # we close the connection here and disassociate
        if self._parent:
            self._parent.closeChild(self)

   
    ##-----------------------------------------
  
    def __activateStream (self, x):

        self.info("connection established in __activateStream")

        # The current generation needs to be wrapped into this hook;
        # this way we don't close the next generation of connection
        # in the case of a reconnect....
        w = ClearStreamWrapper(x, self, self.__nextGeneration())
        self._stream_w = w

        # If optional hooks were specified, call them here; give as an
        # argument the new StreamWrapper so that way the subclass can
        # issue closes on the connection
        if self._hooks and self._hooks.connected:
            self._hooks.connected(w)

        # Launch a reader thread
        w.launchReader()

    ##-----------------------------------------

    ##-----------------------------------------

    def __connectCriticalSection(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ok = False
        try:
            print("critical section .. calling connect...")
            rc = s.connect(tuple(self._remote))
            print("critical section .. done with it! {0}".format(rc))
            self.__activateStream(s)
            ok = True
        except socket.error as e:
            self.warn("Error in connection to {0}: {1}"
                .format(str(self._remote), e))
        return ok

    ##-----------------------------------------
    # To fulfill the packetizer contract, the following...
  
    def rawWrite (self, msg):
        if not self._stream_w:
            self.warn("write attempt with no active stream")
        else:
            self._stream_w.write(msg)
 

##=======================================================================

class RobustTransport (Transport):
    """
    Takes the same parameters as Transport above, but also
    some additionals to tweak the behavior of the 'robust'
    features:
  
    reconnect_delay -- the number of seconds to delay between attempts
        to reconnect to a downed server.
   
    queue_max -- the limit to how many calls we'll queue while we're
        waiting on a reconnect.
   
    warn_threshhold -- if a call takes more than this number of seconds,
        a warning will be fired when the RPC completes.
  
    error_threshhold -- if a call *is taking* more than this number of
        seconds, we will make an error output while the RPC is outstanding,
        and then make an error after we know how long it took.
    """

    def __init__ (self, port=None, host=None, tcp_opts={}, stream=None, log_obj=None,
                  parent=None, hooks={}, dbgr=None, 
                  reconnect_delay=1, queue_max=1000,
                  warn_threshhold=3.0, error_threshhold=10.0):

        Transport.__init__(self, port=port, host=host,
            tcp_opts=tcp_opts, stream=stream, log_obj=log_obj,
            parent=parent, hooks=hooks, dbgr=dbgr)

        self._queue_max = queue_max
        self._reconnect_delay = reconnect_delay
        self._warn_threshhold = warn_threshhold
        self._error_threshhold = error_threshhold
        self._time_rpcs = (self.warn_treshhold or self._error_threshhold)
        self._condition = threading.Condition()
   
    ##-----------------------------------------

    def __pokeQueue (self):
        self._condition.acquire()
        self._condition.notifyAll()
        self._condition.release()

    ##-----------------------------------------

    def __reconnect(self, first_time):
        if not self._explicit_close:
            self.__connectLoop(first_time)

    ##-----------------------------------------

    def __connectLoop(self, first_time):
        prfx = "" if first_time else "re"
        i = 0

        self._lock.acquire() 

        go = True
        while go:
            i += 1
            if self.isConnected() or self._explicit_close:
                go = False
            else:
                self.info("{0}connecting (attempt {1})".format(prfx, i))
                ok = self.__connectCriticalSection()
                if ok:
                    go = False
                else:
                    time.sleep(self._reconnect_delay)

        if self.isConnected():
            s = "" if (i is 1) else "s"
            self.warn("{0}connected after {1} attempt{2}".format(prfx, i, s))   
            self.__pokeQueue()

        self._lock.release()

    ##-----------------------------------------

    def __timedInvoke (self, program, method, arg, notify):

        eth = self._error_threshhold
        wth = self._warn_threshhold
        timer = None
        m = self.makeMethod(program,method)

        def __do_timeout():
            if inv:
                self.error("RPC call to '{0}' is taking > {1}s".format(m, eth))
                timer = None

        if eth:
            timer = threading.Timer(eth, __do_timeout)

        start = time.time()
        inv = self.newInvocation(program=program, method=method, arg=arg, notify=notify)
        ret = inv.call()
        dur = time.time() - start

        if eth and eth <= dur: fn = self.error
        elif wth and wth <= dur: fn = self.warn
        else: fn = None

        if fn:
            fn("RPC call to '{0}' finished in {1}s".format(m, dur))

        if timer:
            timer.cancel()

        return ret

    ##-----------------------------------------

    def __waitInQueue(self):
        self._condition.acquire()
        self._n_waiters += 1
        self._condition.wait()
        self._n_waiters -= 1
        self._condition.release()

    ##-----------------------------------------

    def invoke(self, **kwargs):
        meth = self.makeMethod(kwargs.get("program"), kwargs.get("method"))
        ret = None
        go = True
        while go:
            go = False
            if self.isConnected():
                if self._time_rpcs:
                    ret = self.__timedInvoke(**kwargs)
                else:
                    ret = Transport.invoke(self, **kwargs)
            elif self._explicit_close:
                self.warn("Invoked call to '{0}' after explicit close".format(meth))
            elif self._n_waiters < self._queue_max:
                self.__waitInQueue()
                go = True
            else:
                self.warn("Queue overflow at '{0}'".format(meth))
        return ret
  
##=======================================================================

def createTransport(**kwargs):
    if kwargs.get('robust'):
        ret = RobustTransport(**kwargs)
    else:
        ret = Transport(**kwargs)
    return ret

##=======================================================================

