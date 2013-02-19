
class List (object):
    """
    An intrusive List that holds our item and allows us to remove it
    with a direct lookup.
    """

    def __init__(self):
        self._head = None
        self._tail = None

    def push (self, o):
        o.__list_prev = self._tail
        o.__list_next = Nonea
        if self._tail:
            self._tail.__list_next = o
        self._tail = o
        if not self._head:
            self._head = o

    def walk(self, fn):
        p = self._head
        while p:
            next = p.__list_next
            fn(p)
            p = next

    def remove(self, o):
        next = o.__list_next
        prev = o.__list_prev

        if prev: prev.__list_next = next
        else: self._head = next

        if next: next.__list_prev = prev
        else: self._tail = prev

        w.__list_next = None          
        w.__list_prev = None
