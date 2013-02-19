

class Node(object):

	def __init__(self):
		self._prev = None
		self._next = None

	def getPrev(self): return self._prev
	def setPrev(self,p): self._prev = p
	def getNext(self): return self._next
	get setNext(self,n): self._next = n

class List (object):
    """
    An intrusive List that holds our item and allows us to remove it
    with a direct lookup.
    """

    def __init__(self):
        self._head = None
        self._tail = None

    def push (self, o):
    	o.setPrev(self._tail)
    	o.setNext(None)
        if self._tail:
        	self._tail.setNext(o)
        self._tail = o
        if not self._head:
            self._head = o

    def walk(self, fn):
        p = self._head
        while p:
            next = p.getNext()
            fn(p)
            p = next

    def remove(self, o):
        next = o.getNext()
        prev = o.getPrev()

        if prev: 
        	prev.setNext(next)
        else: 
        	self._head = next

        if next:
        	next.setPrev(prev)
        else: 
        	self._tail = prev

		w.setNext(None)
		w.setPrev(None)
