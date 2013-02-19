
import socket
import log
import dispatch
import debug
import threading


##=======================================================================

class ClearStreamWrapper (object):
	"""
	A shared wrapper around a socket, for which close() is idempotent. Of course,
	no encyrption on this interface.
	"""
	def __init__ (self, socket, parent):
		self.socket = socket
		self.parent = parent
		self.generation = parent.__nextGeneration()
		self.write_closed_warn = False

	def close (self):
		"""
		Return True if we did the actual close, and false otherwise
		"""
		ret = False
		if self.socket:
			ret = True
			x = self.socket
			self.socket = None
			self.parent.__dispatchReset()
			self.parent.__packetizerReset()
			x.close()
		return ret

	def write (self, msg):
		"""
		Write the message to the socket, calling send(2) repeatedly
		until the buffer is flushed out.  Use low-level socket calls.
		"""
		if self.socket:
			self.socket.sendall(msg)
		elif not self.write_closed_warn:
			self.write_closed_warn = True
			self.parent._warn "write on closed socket"

	def stream (self): return self.socket
	def isConnected (self): return not not self.socket
	def getGeneration (self): return self.generation

	def remoteAddress (self):
		return self.socket.getpeername()[0] if self.socket else None
	def reportPort (self):
		return self.socket.getpeername()[1] if self.socket else None
	def remotePeer (self):
		return self.socket.getpeername() if self.socket else None

##=======================================================================

class Transport (dispatch.Dispatch):

	def __init__ (self, port=None, host=None, tcp_opts={}, 
				  stream=None, log_obj=None, parent=None,
				  hooks={}, dbgr=None):

		self._host = "localhost" if (not host or host is "-") else host
		self._port = port
		self._tcp_opts = tcp_opts
		self._explicit_close = False
		self._remote_str = ":".join([self.host,port])

		self.setLogger(log_obj)
		self._generation = 1
		self._lock = threading.Lock()
		self._dbgr = dbgr
		self._hooks = hooks
    
	    # We don't store the TCP stream directly, but rather, sadly,
	    # a **wrapper** around the TCP stream.  This is to make stream
	    # closes idempotent. The case we want to avoid is two closes
	    # on us (due to errors), but the second closing the reconnected
	    # stream, rather than the original stream.  This level of
	    # indirection solves this.
	    self._stream_w = null

	    # potentially set @_tcpw to be non-null
	    if stream:
		    self.__activateStream(stream)

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
	    self._generation++
    	return ret
 
	##-----------------------------------------

	def getGeneration (self):
 	  	return self._stream_w.getGeneration() if self._stream_w else -1 
 
	##-----------------------------------------

	def remoteAddress (self):
	  	return self._stream_w.remoteAddress() if self._stream_w else None
	def remotePort (self):
	  	return self._stream_w.remotePort() if self._stream_w else None
  	def remotePeer (self):
  		return self._stream_w.remotePeer() if self._stream_w else None

	##-----------------------------------------

	def setLogger : (o) ->
		if not o:
			o = log.newDefaultLoggger()
		self._log_obj = o
		self._log_obj.setRemote(self._remote_str)

	##-----------------------------------------

	def getLogger (self): return self._log_obj

	##-----------------------------------------

	def isConnected (self):
		if self._stream_w: return self._stream_w.isConnected()
		else: 	           return False

	##-----------------------------------------

	def connect (self):
		self._lock.acquire()
		if not self.isConnected():
			ret = self.__connectCriticalSection()
		else:
			ret = True
		self._lock.release()
		if not ret:
			self.__backgroundReconnectLoop(True)
		return ret

	##-----------------------------------------

	def __backgroundReconnectLoop(self, first):
		"""
		The standard transport won't try to reconnect....
		"""
		return False

	##-----------------------------------------

	def reset(self, w):
		if not w: w = self._stream_w
		self.__close(w)

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

	def __warn(e) : self._log_obj.warn(e)
	def __info(e) : self._log_obj.info(e)
	def __fatal(e): self._log_obj.fatal(e)
	def __debug(e): self._log_obj.debug(e)
	def __error(e): self._log_obj.error(e)
  
	##-----------------------------------------

	def __close (self, tcpw):
	  # If an optional close hook was specified, call it here...
    if (self._hooks and self._hooks.eof):
      self._hooks.eof(tcpw)
		if tcpw.close():
			self.__backgroundReconnectLoop(False)

	##-----------------------------------------

	def __handleError(self, e, tcpw):
		self.__error(e)
		self.__close(tcpw)

  ##-----------------------------------------
 
  def __packetizeError (self, err): 
    # I think we'll always have the right TCP stream here
    # if we grab the one in the this object.  A packetizer
    # error will happen before any errors in the underlying
    # stream
    self.__handleError("In packetizer: {0}".format(err), self._stream_w)
    
  ##-----------------------------------------

  def __handleClose (self, tcpw):
    if not self._explicit_close:
      self.__info("EOF on transport")
      self.__close(tcpw)

    # for TCP connections that are children of Listeners,
    # we close the connection here and disassociate
    if self._parent:
      self._parent.closeChild(self)
   
  ##-----------------------------------------
  
  def __activateStream (self, x):

    self.__info("connection established")

    # The current generation needs to be wrapped into this hook;
    # this way we don't close the next generation of connection
    # in the case of a reconnect....
    w = ClearStreamWrapper(x, self)
    self._stream_w = w
    
    # If optional hooks were specified, call them here; give as an
    # argument the new StreamWrapper so that way the subclass can
    # issue closes on the connection
    if self._hooks and self._hooks.connected:
      self._hooks.connected(w)

    #
    # MK 2012/12/20 -- Revisit me!
    # 
    # It if my current belief that we don't have to listen to the event
    # 'end', because a 'close' event will always follow it, and we do
    # act on the close event. The distance between the two gives us
    # the time to act on a TCP-half-close, which we are not doing.
    # So for now, we are going to ignore the 'end' and just act
    # on the 'close'.
    # 
    self.__readLoop()

  ##-----------------------------------------

  def __readLoop (self):

    self._lock.acquire()
    go = True

    w = self._stream_w

    while go and w:
      try:
        buf = w.recv()
        if buf:
          self.packetizeData(buf)
        else:
          self.__handleClose(w)
          go = False
      except IOError as e:
        self.__handleError(e, w)
        go = False
    self._lock.release()

  ##-----------------------------------------

  def __connectCriticalSection(self):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ok = FAlse
    try:
      s.connect((self.host, self.port))
      self.__activateStream(s)
      ok = True
    except socket.error as e:
      self.__warn("Error in connection to {0}: {1}"
          .format(self._remote_str, e))

    return ok

  ##-----------------------------------------
  # To fulfill the packetizer contract, the following...
  
  def rawWrite (msg, encoding):
    if not self._stream_w:
      self.__warn("write attempt with no active stream")
    else:
      self._stream_w.write(msg)
 
  ##-----------------------------------------

##=======================================================================

exports.RobustTransport = class RobustTransport extends Transport
   
  ##-----------------------------------------

  # Take two dictionaries -- the first is as in Transport,
  # and the second is configuration parameters specific to this
  # transport.
  #
  #    reconnect_delay -- the number of seconds to delay between attempts
  #       to reconnect to a downed server.
  # 
  #    queue_max -- the limit to how many calls we'll queue while we're
  #       waiting on a reconnect.
  # 
  #    warn_threshhold -- if a call takes more than this number of seconds,
  #       a warning will be fired when the RPC completes.
  # 
  #    error_threshhold -- if a call *is taking* more than this number of
  #       seconds, we will make an error output while the RPC is outstanding,
  #       and then make an error after we know how long it took.
  #
  #      
  constructor : (sd, d = {}) ->
    super sd
    
    { @queue_max, @warn_threshhold, @error_threshhold } = d

    # in seconds, provide a default of 1s for a reconnect delay
    # if none was given.  Also, 0 is not a valid value.
    @reconnect_delay = if (x = d.reconnect_delay) then x else 1

    # For @queue_max, a value of '0' means don't queue, but a null
    # or unspecifed value means use a reasonable default, which we
    # supply here as 1000.
    @queue_max = 1000 unless @queue_max?
    
    @_time_rpcs = @warn_threshhold? or @error_threshhold?
    
    @_waiters = []
   
  ##-----------------------------------------

  _reconnect : (first_time) ->
    # Do not reconnect on an explicit close
    @_connect_loop first_time if not @_explicit_close

  ##-----------------------------------------

  _flush_queue : () ->
    tmp = @_waiters
    @_waiters = []
    for w in tmp
      @invoke w...
  
  ##-----------------------------------------
 
  _connect_loop : (first_time = false, cb) ->
    prfx = if first_time then "" else "re"
    i = 0
    
    await @_lock.acquire defer()

    go = true
    while go
      i++
      if @is_connected() or @_explicit_close
        go = false
      else
        @_info "#{prfx}connecting (attempt #{i})"
        await @_connect_critical_section defer err
        if err?
          await setTimeout defer(), @reconnect_delay*1000
        else
          go = false
    
    if @is_connected()
      s = if i is 1 then "" else "s"
      @_warn "#{prfx}connected after #{i} attempt#{s}"
      @_flush_queue()
      
    @_lock.release()
    cb() if cb

  ##-----------------------------------------

  _timed_invoke : (arg, cb) ->

    [ OK, TIMEOUT ] = [0..1]
    tm = new Timer start : true
    rv = new iced.Rendezvous
    meth = @make_method arg.program, arg.method

    et = if @error_threshhold then @error_threshhold*1000 else 0
    wt = if @warn_threshhold then @warn_threshhold*1000 else 0

    # Keep a handle to this timeout so we can clear it later on success
    to = setTimeout rv.id(TIMEOUT).defer(), et if et

    # Make the actual RPC
    Dispatch.prototype.invoke.call @, arg, rv.id(OK).defer rpc_res...

    # Wait for the first one...
    await rv.wait defer which

    # will we leak memory for the calls that never come back?
    flag = true
    
    while flag
      if which is TIMEOUT
        @_error "RPC call to '#{meth}' is taking > #{et/1000}s"
        await rv.wait defer which
      else
        clearTimeout to
        flag = false

    dur = tm.stop()

    m =  if et and dur >= et then @_error
    else if wt and dur >= wt then @_warn
    else                     null

    m.call @, "RPC call to '#{meth}' finished in #{dur/1000}s" if m

    cb rpc_res...
   
  ##-----------------------------------------

  invoke : (arg, cb) ->
    meth = @make_method arg.program, arg.method
    if @is_connected()
      if @_time_rpcs then @_timed_invoke arg, cb
      else                super arg, cb
    else if @_explicit_close
      @_warn "invoke call after explicit close"
      cb "socket was closed", {}
    else if @_waiters.length < @queue_max
      @_waiters.push [ arg, cb ]
      @_info "Queuing call to #{meth} (num queued: #{@_waiters.length})"
    else if @queue_max > 0
      @_warn "Queue overflow for #{meth}"
  
##=======================================================================

exports.createTransport = (opts) ->
  if opts.robust then new RobustTransport opts, opts
  else                new Transport opts

##=======================================================================

