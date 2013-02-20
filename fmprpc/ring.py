
from err import RingError

##=======================================================================

class Ring (object):
    """
    A simple ring buffer for reading in data from network.
    Every so often we'll compress smaller buffers into bigger
    buffers, but try to limit that....
    """

    def __init__ (self):
        self._bufs = []
        self._len = 0

    def buffer (self, b):
        self._bufs.append(b)
        self._len += len(b)

    def __len__ (self):
        return self._len

    #-----------------------------------------

    def grab (self, n_wanted):
        """Grab n_wanted bytes from the buffer as one continguous
        bytearray.  This means we'll be concating a bunch of smaller
        buffers into a bigger one.
        """

        if n_wanted > len(self): 
            buf = None
        elif len(self._bufs) and len(self._bufs[0]) >= n_wanted: 
            buf = self._bufs[0]
        else:
            n_grabbed = 0
            num_bufs = 0

            for b in self._bufs:
                n_grabbed += len(b)
                num_bufs += 1
                if n_grabbed >= n_wanted:
                    break

            # now make a buffer that's potentially bigger than the one we
            # wanted
            buf = bytearray(0)

            for b in self._bufs[0:num_bufs]:
                buf += b

            # the first buffer we'll keep
            first_pos = num_bufs - 1
            self._bufs[first_pos] = buf
            self._bufs = self._bufs[first_pos:]

        # Make sure to truncate the data and not to return too much....
        ret = buf[0:n_wanted] if buf else None
        return ret

    #-----------------------------------------
    
    def consume(self, n):
        """
        Consume n bytes from the buffer Ring. Assumes that grab(n)
        has successfully been called, since it will only try to 
        remove the n bytes from one continguous array.
        """
        if len(self._bufs) is 0 or len(self._bufs[0]) < n:
            raise RingError("underflow; can't remove {0} bytes".format(n))
        b = self._bufs[0]
        if len(b) is n:
            self._bufs = self._bufs[1:]
        else:
            self._bufs[0] = b[n:]
        self._len -= n
