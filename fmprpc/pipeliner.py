
import threading
from collections import deque

class Runner(threading.Thread):

    def __init__ (self, runfn, resfn):
        threading.Thread.__init__(self)
        self._runfn = runfn
        self._resfn = resfn

    def run(self):
        res = self._runfn()
        self._resfn(res)

class Pipeliner (object):

    def __init__ (self, w):
        self._width = w
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._queue = deque()
        self._slots = {}
        self._out = 0
        self._i = 0
        self._done = False
        self._flushed = threading.Event()

    def isRoomInWindow(self):
        return (self._out < self._width)

    def __genResultFunction(self, i):
        def ret (res):
            self._lock.acquire()
            self._slots[i] = res
            self._out -= 1
            self._cond.notify()
            self._lock.release()
        return ret

    def __launch (self, func):
        i = self._i
        self._i += 1
        self._out += 1
        t = Runner(func, self.__genResultFunction(i))
        t.start()

    def airTrafficControl (self):
        self._lock.acquire()
        while (not self._done) or len(self._queue) or (self._out > 0):
            self._cond.wait()
            while self.isRoomInWindow() and len(self._queue):
                p = self._queue.popleft()
                self.__launch(p)
        self._lock.release()
        self._flushed.set()

    def flush (self):
        self._lock.acquire()
        self._done = True
        self._cond.notify()
        self._lock.release()
        self._flushed.wait()
        return self._slots

    def push (self, func):
        if self._done:
            raise err.PipelinerError("Cannot push new events after flush() call")
        self._lock.acquire()
        if self.isRoomInWindow():
            self.__launch(func)
        else:
            self._queue.append(func)
        self._lock.release()

    def start (self):
        # Launch the background air traffic controller thread...
        threading.Thread(target=self.airTrafficControl).start()


