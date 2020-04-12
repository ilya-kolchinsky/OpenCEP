"""
lower bound and the upper bound
"""
from abc import ABC
from abc import abstractmethod
from collections.abc import MutableSequence

# from blist import blist B+ tree like list


class Storage(MutableSequence):
    """
    This stores all the partial matches
    """

    def __init__(self):
        self._container = None

    def insert(self, index, item):  # abstract in MutableSequence      for x.insert(i,a)
        self._container[index] = item

    def __setitem__(self, index, item):  # abstract in MutableSequence      for x[i] = a
        self._container[index] = item

    def __getitem__(self, index):  # abstract in MutableSequence      for y = x[i]
        return self._container[index]

    def __len__(self):  # abstract in MutableSequence      for len(x)
        return len(self._container)

    def __delitem__(self, index):  # abstract in MutableSequence      for del x[i]
        del self._container[index]

    @abstractmethod
    def fake(self):  # just to avoid problems because of _container = None
        pass

    # FOR ITERATING:
    # def __contains__(self, item):
    #     return item in self._container
    # def __iter__(self):
    #     return iter(self._container)
    # FOR CONCATENATING:
    # def __add_(self, rhs):
    #     return Storage(chain(self._container, rhs._container))
    def __repr__(self):
        return "Storage contains {}".format(
            repr(self._container) if self._container else " "
        )

    # for tests
    def __eq__(self, rhs):
        if not isinstance(rhs, Storage):
            return NotImplemented
        return self._container == rhs._container

    def __ne__(self, rhs):
        if not isinstance(rhs, Storage):
            return NotImplemented
        return self._container != rhs._container


class ArrayStorage(Storage):
    def __init__(self):
        self._container = []

    def append(self, item):
        self._container.append(item)


# class BListStorage(Storage):
#     def __init__(self):
#         self._container = blist()
# https://books.google.co.il/books?id=jnEoBgAAQBAJ&pg=A119&lpg=PA119&dq=difference+between+__setitem__+and+insert+in+python&source=bl&ots=5WjkK7Acbl&sig=ACfU3U06CgfJju4aTo8K20rhq0tIul6oBg&hl=en&sa=X&ved=2ahUKEwjo9oGLpuHoAhVTXMAKHf5jA68Q6AEwDnoECA0QOw#v=onepage&q=difference%20between%20__setitem__%20and%20insert%20in%20python&f=false
