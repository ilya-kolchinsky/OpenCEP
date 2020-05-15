"""
lower bound and the upper bound
"""
from abc import abstractmethod
from collections.abc import MutableSequence
from itertools import chain
import bisect
from misc.Utils import get_first_index, get_last_index
from datetime import datetime
from typing import List
from evaluation.PartialMatch import PartialMatch
from misc.Utils import find_partial_match_by_timestamp

# from blist import blist B+ tree like list
# TODO Note: for mutable collections like ours we shouldn't return self but a copy of self.
# if we were immutable we could return self because the user aslan can't use our collection


class Storage(MutableSequence):
    """
    This stores all the partial matches
    """

    # TODO def get_parial_matches(self, pm):
    @abstractmethod
    def __init__(self):
        self._container: MutableSequence
        self._key: callable

    def append(self, pm):
        self._container.append(pm)

    def insert(self, index, item):  # abstract in MutableSequence      for x.insert(i,a)
        self._container.insert(index, item)

    def get_key(self):
        return self._key

    def __setitem__(self, index, item):  # abstract in MutableSequence      for x[i] = a
        self._container[index] = item

    # TODO maybe make a get_item for both classes with deepcopy but I think if we only iterate over the result we then don't need to construct a new one and we can return the _container itself.
    def __getitem__(self, index):  # abstract in MutableSequence      for y = x[i]
        # return self._container[index] => not very good. index can be a slice [:] which makes us return a list when we should return an SortedStorage object
        result = self._container[index]
        return result  # return SortedStorage(result, self._key) if isinstance(index, slice) else result

    def __len__(self):  # abstract in MutableSequence      for len(x)
        return len(self._container)

    def __delitem__(self, index):  # abstract in MutableSequence      for del x[i]
        del self._container[index]

    def __contains__(self, item):
        return item in self._container  # todo : we can use bisect here instead

    def __iter__(self):
        return iter(self._container)

    def __add__(self, rhs):  #      for s1 + s2
        pass

    # TODO: we can implement the index method(overriding it actually) and other methods with BINARY SEARCH.
    """def index(self, item):
           index = bisect_left(self._container, item)
           if (index != len(self._container)) and (self._container[index] == item):
                return index
            raise ValueError("{} not found".format(repr(item)))
        if we implement this function then we can let __contains__ call it to to be O(logn)
    """

    # FOR TESTS
    def __repr__(self):
        return "Storage contains {}".format(self._container if self._container else "Nothing")

    def __eq__(self, rhs):
        if not isinstance(rhs, Storage):
            return NotImplemented
        return self._container == rhs._container

    def __ne__(self, rhs):
        if not isinstance(rhs, Storage):
            return NotImplemented
        return self._container != rhs._container


class SortedStorage(Storage):
    def __init__(self, key, relop=None, equation_side=None, sort_by_first_timestamp=False):
        self._container = []
        self._key = key
        self._sorted_by_first_timestamp = sort_by_first_timestamp
        self._get_function = self._choose_get_function(relop, equation_side)

    """
    def __repr__(self):
        return "Storage contains {}, key is {}, get_func is ".format(
            self._container if self._container else "Nothing", self._key
        )
    """

    def add(self, pm):
        index = get_last_index(self._container, self._key(pm), self._key)
        index = 0 if index == -1 else index
        self._container.insert(index, pm)

    def get(self, value):
        if len(self._container) == 0:
            return []
        return self._get_function(value)

    # this also can return many values
    def _get_equal(self, value):
        left_index = get_first_index(self._container, value, self._key)
        if left_index == len(self._container) or left_index == -1 or self._key(self._container[left_index]) != value:
            return []

        right_index = get_last_index(self._container, value, self._key)
        return self._container[left_index : right_index + 1]

    def _get_unequal(self, value):
        left_index = get_first_index(self._container, value, self._key)
        if left_index == len(self._container) or left_index == -1 or self._key(self._container[left_index]) != value:
            return self._container  # might need to return a copy
        right_index = get_last_index(self._container, value, self._key)
        return self._container[:left_index] + self._container[right_index + 1 :]
        # can use extend or itertools.chain see what's best

    def _get_greater(self, value):
        right_index = get_last_index(self._container, value, self._key)
        if right_index == len(self._container):
            return []
        if right_index == -1:
            return self._container  # might need to return a copy
        if self._key(self._container[right_index]) != value:
            return self._container[
                right_index:
            ]  # in case value doesn't exist left_index will point on the first one greater than it
        return self._container[right_index + 1 :]

    def _get_smaller(self, value):
        left_index = get_first_index(self._container, value, self._key)
        if left_index == len(self._container):
            return self._container
        if left_index == -1:
            return []  # might need to return a copy
        if self._key(self._container[left_index]) != value:
            return self._container[
                : left_index + 1
            ]  # in case value doesn't exist left_index will point on the first one smaller than it
        return self._container[:left_index]

    def _get_greater_or_equal(self, value):
        return self._get_equal(value) + self._get_greater(value)
        # maybe I can do better by not calling these functions

    def _get_smaller_or_equal(self, value):
        return self._get_smaller(value) + self._get_equal(value)

    # implementing add or extend depends on whether we want a new object or update the current SortedStorage.
    # if we need something we'd need extend with O(1)
    def __add__(self, rhs):  #      for s1 + s2
        # return SortedStorage(list(chain(self._container, rhs._container)), self._key)
        return self._container + rhs._container

    def clean_expired_partial_matches(self, last_timestamp: datetime):
        if self._sorted_by_first_timestamp:
            count = find_partial_match_by_timestamp(self._container, last_timestamp)
            # self._container = self._container[count:]
            del self._container[:count]  # MUH
        else:  # filter returns a generator
            self._container = list(filter(lambda pm: pm.first_timestamp >= last_timestamp, self._container))

    def _choose_get_function(self, relop, equation_side):
        assert relop is not None
        if relop == "==":
            return self._get_equal
        if relop == "!=":
            return self._get_unequal

        if relop == ">":
            return self._get_greater if equation_side == "left" else self._get_smaller
        if relop == "<":
            return self._get_smaller if equation_side == "left" else self._get_greater

        if relop == ">=":
            return self._get_greater_or_equal if equation_side == "left" else self._get_smaller_or_equal
        if relop == "<=":
            return self._get_smaller_or_equal if equation_side == "left" else self._get_greater_or_equal


class UnsortedStorage(Storage):
    def __init__(self):
        self._container = []
        self._key = lambda x: x
        self._sorted_by_first_timestamp = False

    def get(self, value):
        return self._container

    # the same as def append()
    def add(self, pm):
        self._container.append(pm)

    def clean_expired_partial_matches(self, last_timestamp: datetime):
        self._container = list(filter(lambda pm: pm.first_timestamp >= last_timestamp, self._container))


# https://books.google.co.il/books?id=jnEoBgAAQBAJ&pg=A119&lpg=PA119&dq=difference+between+__setitem__+and+insert+in+python&source=bl&ots=5WjkK7Acbl&sig=ACfU3U06CgfJju4aTo8K20rhq0tIul6oBg&hl=en&sa=X&ved=2ahUKEwjo9oGLpuHoAhVTXMAKHf5jA68Q6AEwDnoECA0QOw#v=onepage&q=difference%20between%20__setitem__%20and%20insert%20in%20python&f=false
