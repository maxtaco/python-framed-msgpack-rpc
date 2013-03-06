import socket
import log
import dispatch
import debug
import threading
import time
import ilist
import util
import weakref
import sys
import address

##=======================================================================

class ConstantReader (threading.Thread, log.Base):
    """
    A thread that will read on a socket, injecting any new data packets
    into the packetizer.  It's important to note that this class doesn't
    get a real reference to the Transport it's working on behalf of, only
    a weak reference to it.  This way it won't keep a transport from
    going out of scope in the case of a client who has stop being 
    interested in a particular connection.
    """
    def __init__ (self, wrapper):
        self.wrapper = wrapper
        self.transport = wrapper.transport
        log.Base.__init__(self, self.transport().getLogger())
        threading.Thread.__init__(self)

    def run (self):
        go = True

        while go and self.transport():
            op = None
            try:
                buf = self.wrapper.recv(0x1000)
                self.debug("Got data: {0}".format(util.formatRaw(buf)))
                if buf:
                    op = lambda : self.transport().packetizeData(buf)
                else:
                    op = lambda : self.transport().handleClose(self.wrapper)
                    go = False
            except IOError as e:
                op = lambda : self.transport().handleError(e, self.wrapper)
                go = False

            # Any operations done on the transport should be atomic and protected
            # by locks.  This includes shutting down the socket due to an EOF
            # or an error.
            if op and self.transport():
                self.transport().atomicOp(op)

        self.info("leave reader loop")

##=======================================================================

class ClearStreamWrapper (log.Base):
    """
    A shared wrapper around a socket, for which close() is idempotent. Of course,
    no encyrption on this interface.
    """
    def __init__ (self, s, transport):
        # Disable Nagle by default on all sockets...
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._socket = s
        self.generation = transport().nextGeneration()
        self.write_closed_warn = False
        self.reader = None
        self.transport = transport
        log.Base.__init__(self, transport().getLogger())
        if socket:
            self.remote = address.InternetAddress(tup=s.getpeername())


    def start(self):
        """
        Activate this wrapper.  Do any necessary handshaking and also
        start the persistent reading thread to gather incoming data.
        """
        self.__launchConstantReader()
        return True

    def __launchConstantReader(self):
        if self.reader:
            self.error("Refusing to launch a second reader...")
        else:
            self.reader = ConstantReader(self)
            self.reader.start()

    def shutdownStream (self, x, force):
        if force:
            # Force the shutdown in the case of an explicit close
            # or a transport going out of scope.  Shutting down
            # the socket with this call will cause the ConstantReader
            # loop to see and EOF and to exit.
            x.shutdown(socket.SHUT_RDWR)
        x.close()

    def close (self, force):
        """
        Return True if we did the actual close, and false otherwise
        """
        ret = False
        if self._socket:
            ret = True
            x = self._socket
            self._socket = None
            t = self.transport()
            if t:
                t.dispatchReset()
                t.packetizerReset()

            self.shutdownStream(x, force)
        return ret

    def write (self, msg):
        """
        Write the message to the socket, calling send(2) repeatedly
        until the buffer is flushed out.  Use low-level socket calls.
        """
        self.debug("writing data: {0}".format(util.formatRaw(msg)))
        if self.stream():
            self.stream().sendall(msg)
        elif not self.write_closed_warn:
            self.write_closed_warn = True
            self.warn("write on closed stream")

    def recv(self, n):
        if self.stream():
            return self.stream().recv(n)
        else:
            self.warn("calling recv on a closed socket")
            return None

    def stream (self): return self._socket
    def isConnected (self): return not not self._socket
    def getGeneration (self): return self.generation
    def remote(self): return self.remote

    def authenticatedUid (self):
        """Some subclasses of the clear stream can authenticate users
        are part of the handshake protocol (see crypto.tls) but by
        default, nothing here."""
        return None

##=======================================================================

class Transport (dispatch.Dispatch):
    """
    A wrapper around a TCP stream (given by the parameter socket).
    """

    def __init__ (self, remote=None, tcp_opts={}, 
                  stream=None, log_obj=None, parent=None,
                  hooks={}, dbgr=None):

        # Don't create with a log object, we'll get to that
        # below via setLogger().  We might want to revisit this...
        dispatch.Dispatch.__init__(self, log_obj=None)

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
        self._handshake_error = False
        self._parent = parent

        self._generation = 1
        self._dbgr = dbgr
        self._hooks = hooks

        # Subclasses can change this
        self._wrapper_klass = ClearStreamWrapper
        self._error_messages = {}

        self.setLogger(log_obj)

        # If we have a parent (like a server with child connections)
        # then the parent might want us in a list, and we need to make
        # an ilist Node to accommodate that.
        if self._parent:
            self._node = ilist.Node(self)

        # potentially set self._stream_w
        if stream:
            self.activateStream(stream)

    ##-----------------------------------------

    def serverListNode (self): 
        """Get the ilist.Node data structure used for storing this object
        in a list of other transports on servers.  Servers need to do this
        for two reasons: (1) to keep a reference to all living children, lest
        they get thrown away; and (2) so that it can iterate across all
        children in certain cases (like to turn on debugging).
        """
        return self._node

    ##-----------------------------------------

    def reportError (self, typ, e): self._error_messages[typ] = e
    def getError (self,type): return self._error_messages.get(type)

    ##-----------------------------------------

    def setWrapperClass(self, klass):
        self._wrapper_klass = klass

    ##-----------------------------------------

    def setDebugger (d): self._dbgr = d
  
    ##---------------------------------------

    def setDebugFlags (f):
        self.setDebugger(debug.makeDebugger(d,self.getLogger()))
   
    ##-----------------------------------------

    def nextGeneration (self):
        """To be called by StreamWrapper objects but not by average users."""
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
        """Override of the log.Base.setLogger(), we're doing two additional
        things: first, picking a sensible default logger on None; and second,
        setting the remote field on the logger appropriately.
        """

        if not o:
            o = log.newDefaultLogger()
        log.Base.setLogger(self, o)
        o.setRemote(self.remote())

    ##-----------------------------------------

    def isConnected (self):
        if self._stream_w: return self._stream_w.isConnected()
        else:              return False

    ##-----------------------------------------

    def connect (self):
        self._lock.acquire()
        if not self.isConnected():
            ret = self.__connectCriticalSection()
        else:
            ret = True
        self._lock.release()
        if not ret:
            self.reconnect(True)
        return ret

    ##-----------------------------------------

    def reconnect(self, first):
        """
        The standard transport won't try to reconnect....
        """
        return False

    ##-----------------------------------------

    def reset(self, w):
        self._lock.acquire()
        if not w: w = self._stream_w
        self.__implicitClose(w)
        self._lock.release()

    ##-----------------------------------------

    def __del__(self):
        self.debug("calling __del__ on transport object")
        # An object deletion is equivalent to an explicit close...
        self.__explicitClose()
   
    ##-----------------------------------------

    def close (self):
        """
        Call to explicitly close this connection.  After an explicit close,
        reconnects are not attempted....
        """
        self.__explicitClose()

    #
    # /Public API
    ##---------------------------------------------------


    def __implicitClose (self, tcpw):
        # If an optional close hook was specified, call it here...
        if (self._hooks and self._hooks.eof):
            self._hooks.eof(tcpw)
        if tcpw.close(False):
            self.reconnect(False)
        self.__unregister()

    ##-----------------------------------------

    def __explicitClose(self):
        self._lock.acquire()
        self._explicit_close = True
        if self._stream_w:
            w = self._stream_w
            self._stream_w = None
            w.close(True)
            self.__unregister()
        self._lock.release()

    ##-----------------------------------------

    def __unregister(self):
        if self._parent and self._node:
            self._parent.removeChild(self)
            self._node.clear()
            self._node = None
   
    ##-----------------------------------------

    def atomicOp(self,op):
        self._lock.acquire()
        op()
        self._lock.release()
   
    ##-----------------------------------------

    def handleError(self, e, tcpw):
        self.error(e)
        self.__implicitClose(tcpw)

    ##-----------------------------------------

    def authenticatedUid(self):
        """Ask our active stream if there's an authenticated uid
        on the other end.  Usually no."""
        ret = None
        if self._stream_w:
            ret = self._stream_w.authenticatedUid()
        return ret

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
            self.__implicitClose(tcpw)

    ##-----------------------------------------

    def makeWrapper(self, x):
        #
        # The current generation needs to be wrapped into this hook;
        # this way we don't close the next generation of connection
        # in the case of a reconnect.
        #
        # Also note, and this is **key**: we pass a weakref to ourselves,
        # not a strong ref.  The idea is that we don't want the readloop
        # on the socket itself to keep this stream alive. In the case of 
        # a client-allocated transport, we want to force a close of the 
        # readloop when the client object goes out of scope.  In the
        # case of a reader we want to stay alive as long as the connection
        # stays open, but there's a pointer to us in the server's _children
        # list, so we have no need for the readloop to keep the references
        # afloat.
        #
        # One more thing: this is a one-liner function so we
        # can let subclasses define new behavior here.
        return self._wrapper_klass (x, weakref.ref(self))

    ##-----------------------------------------

    def activateStream (self, x):

        self.info("connection established in activateStream")

        w = self.makeWrapper(x)
        self._stream_w = w

        # Server should catch exceptions in start, but clients won't.
        # In either case, only go ahead if start() returned True.
        if w.start():
            ok = True

            # If optional hooks were specified, call them here; give as an
            # argument the new StreamWrapper so that way the subclass can
            # issue closes on the connection
            if self._hooks and self._hooks.connected:
                self._hooks.connected(w)
        else:
            self.info("start() failed in activateStream()")
            self._handshake_error = True
            ok = False
        return ok

    ##-----------------------------------------

    def __connectCriticalSection(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ok = False
        try:
            rc = s.connect(tuple(self._remote))
            ok = self.activateStream(s)
        except socket.error as e:
            msg = "Error in connection to {0}: {1}".format(str(self._remote), e)
            self.warn(msg)
            self.reportError('connect', msg)
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

    def reconnect(self, first_time):
        if not self._explicit_close and not self._handshake_error:
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

