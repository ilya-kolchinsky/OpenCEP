from abc import abstractmethod
from collections.abc import MutableSequence
from itertools import chain
import bisect
from misc.Utils import get_first_index, get_last_index
from datetime import datetime, timedelta
from typing import List
from evaluation.PartialMatch import PartialMatch
from misc.Utils import find_partial_match_by_timestamp
from base.Formula import RelopTypes
from enum import Enum

class EquationSides(Enum):
    """
    The various RELOPs for a condition in a formula.
    """
    left = 0,
    right = 1
    
class Storage(MutableSequence):
    """
    Abstract class for storing partial matches
    """
    @abstractmethod
    def __init__(self):
        self._container: MutableSequence
        self._key: callable

    def append(self, pm):
        self._container.append(pm)

    def insert(self, index, item):
        self._container.insert(index, item)

    def get_key(self):
        return self._key

    def __setitem__(self, index, item):
        self._container[index] = item

    def __getitem__(self, index):
        # index can be a slice [:] so the return value can be a list
        return self._container[index]

    def __len__(self):
        return len(self._container)

    def __delitem__(self, index):
        del self._container[index]

    def __contains__(self, item):
        # can be implemented with O(logn) if the storage is sorted
        return item in self._container

    def __iter__(self):
        return iter(self._container)

    def __add__(self, rhs):
        if not isinstance(rhs, Storage):
            return NotImplemented
        return self._container + rhs._container

    

class SortedStorage(Storage):
    """
    This class stores the partial matches sorted in increasing order according to a function(key) on partial matches.
    """
    def __init__(self, key, relop, equation_side, clean_up_every: int, sort_by_first_timestamp=False, in_leaf=False):
        self._container = []  # sorted in increasing order according to key
        self._key = key
        self._in_leaf = in_leaf
        self._sorted_by_first_timestamp = sort_by_first_timestamp
        self._clean_up_every = clean_up_every
        self._access_count = 0
        self._get_function = self._choose_get_function(relop, equation_side)


    def add(self, pm):
        self._access_count += 1
        if self._in_leaf and self._sorted_by_first_timestamp:
            self._container.append(pm)
        else:
            index = get_last_index(self._container, self._key(pm), self._key)
            index = 0 if index == -1 else index
            self._container.insert(index, pm)

    def get(self, value):
        if len(self._container) == 0:
            return []
        return self._get_function(value)

    def try_clean_expired_partial_matches(self, timestamp: datetime):
        if self._access_count >= self._clean_up_every:
            self._clean_expired_partial_matches(timestamp)
            self._access_count = 0

    def _clean_expired_partial_matches(self, timestamp: datetime):
        """
        Removes partial matches whose earliest timestamp violates the time window constraint.
        """
        if self._sorted_by_first_timestamp:
            count = find_partial_match_by_timestamp(self._container, timestamp)
            self._container = self._container[count:]
        else:
            self._container = list(filter(lambda pm: pm.first_timestamp >= timestamp, self._container))

    def _get_equal(self, value):
        left_index = get_first_index(self._container, value, self._key)
        if left_index == len(self._container) or left_index == -1 or self._key(self._container[left_index]) != value:
            return []
        right_index = get_last_index(self._container, value, self._key)
        return self._container[left_index : right_index + 1]

    def _get_unequal(self, value):
        left_index = get_first_index(self._container, value, self._key)
        if left_index == len(self._container) or left_index == -1 or self._key(self._container[left_index]) != value:
            return self._container
        right_index = get_last_index(self._container, value, self._key)
        return self._container[:left_index] + self._container[right_index + 1 :]

    def _get_greater(self, value):
        right_index = get_last_index(self._container, value, self._key)
        if right_index == len(self._container):
            return []
        if right_index == -1:
            return self._container
        # in case value doesn't exist right_index will point on the first one greater than it
        if self._key(self._container[right_index]) != value:
            return self._container[right_index:] 
        return self._container[right_index + 1 :]

    def _get_smaller(self, value):
        left_index = get_first_index(self._container, value, self._key)
        if left_index == len(self._container):
            return self._container
        if left_index == -1:
            return []
        # in case value doesn't exist left_index will point on the first one smaller than it
        if self._key(self._container[left_index]) != value:
            return self._container[: left_index + 1]
            
        return self._container[:left_index]

    def _get_greater_or_equal(self, value):
        return self._get_equal(value) + self._get_greater(value)

    def _get_smaller_or_equal(self, value):
        return self._get_smaller(value) + self._get_equal(value)

    def _choose_get_function(self, relop: RelopTypes, equation_side):
        assert relop is not None
        if relop == RelopTypes.Equal:
            return self._get_equal
        if relop == RelopTypes.NotEqual:
            return self._get_unequal

        if relop == RelopTypes.Greater:
            return self._get_greater if equation_side == EquationSides.left else self._get_smaller
        if relop == RelopTypes.Smaller:
            return self._get_smaller if equation_side == EquationSides.left else self._get_greater

        if relop == RelopTypes.GreaterEqual:
            return self._get_greater_or_equal if equation_side == EquationSides.left else self._get_smaller_or_equal
        if relop == RelopTypes.SmallerEqual:
            return self._get_smaller_or_equal if equation_side == EquationSides.left else self._get_greater_or_equal


class UnsortedStorage(Storage):
    """
    This class stores partial matches unsorted.
    It is used when it's difficult to specify an order that helps when receiving partial mitches.
    """
    def __init__(self, clean_up_every: int, in_leaf=False):
        self._container = []
        self._key = lambda x: x
        self._in_leaf = in_leaf
        self._clean_up_every = clean_up_every
        self._access_count = 0

    def get(self, value):
        return self._container

    def add(self, pm):
        self._access_count += 1
        self._container.append(pm)
        

    def try_clean_expired_partial_matches(self, timestamp: datetime):
        if self._access_count >= self._clean_up_every:
            self._clean_expired_partial_matches(timestamp)
            self._access_count = 0

    def _clean_expired_partial_matches(self, timestamp: datetime):
        if self._in_leaf:
            count = find_partial_match_by_timestamp(self._container, timestamp)
            self._container = self._container[count:]
        else:
            self._container = list(filter(lambda pm: pm.first_timestamp >= timestamp, self._container))


class TreeStorageParameters:
    """
    Parameters for the evaluation tree to specify how to store the data.
    for future compatability - can contain fields to be passed to the Tree constructor.
    """
    def __init__(
        self,
        sort_storage: bool = False,
        attributes_priorities: dict = {},
        clean_expired_every: int = 100,
    ):
        if sort_storage is None:
            sort_storage = False

        if attributes_priorities is None:
             attributes_priorities = {}

        if clean_expired_every <= 0:
            raise Exception('clean_expired_every should be positive.')

        self.sort_storage = sort_storage
        self.attributes_priorities = attributes_priorities
        self.clean_expired_every = clean_expired_every
