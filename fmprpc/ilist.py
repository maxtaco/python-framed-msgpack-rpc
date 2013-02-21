
import weakref

class Node(object):
	"""
	A little wrapper node for use in inserting stuff into the List below...
	"""

	def __init__(self, parent):
		self._prev = None
		self._next = None
		self._parent = parent

	def getPrev(self): return self._prev
	def setPrev(self,p): self._prev = p
	def getNext(self): return self._next
	def setNext(self,n): self._next = n
	def getParent(self): return self._parent

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
			o = p.getParent()
			if o:
				fn(o)
			p = next

	def remove(self, o):
		print("ilist remove!")
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

		o.setNext(None)
		o.setPrev(None)
