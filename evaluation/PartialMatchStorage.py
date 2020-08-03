from evaluation.PartialMatch import PartialMatch
from misc.Utils import get_first_index, get_last_index
from datetime import datetime
from misc.Utils import find_partial_match_by_timestamp
from base.Formula import RelopTypes, EquationSides

    
class PartialMatchStorage:
    """
    Abstract class for storing partial matches.
    A container for partial matches is different from the typical container in one regard: upon access, it returns a
    subset of all stored partial matches that are deemed relevant to the given key (might return an empty list, a list
    containing a single object, multiple objects, or even the entire stored content). This behavior contradicts the
    "regular" container behavior fetching a single value corresponding to this key.
    """
    def __init__(self, get_match_key: callable, sorted_by_arrival_order: bool, clean_up_interval: int):
        self._partial_matches = []
        if get_match_key is None:
            self._get_key = lambda x: x
        else:
            self._get_key = get_match_key
        self._sorted_by_arrival_order = sorted_by_arrival_order
        self._clean_up_interval = clean_up_interval
        self._access_count = 0

    def get_key_function(self):
        """
        Returns the function used in order to calculate the sorting key.
        """
        return self._get_key

    def __len__(self):
        """
        Returns the number of the currently stored partial matches.
        """
        return len(self._partial_matches)

    def __setitem__(self, index, item):
        """
        Implements list-style "set item" semantics.
        """
        self._partial_matches[index] = item

    def __getitem__(self, index):
        """
        Implements list-style "get item" semantics.
        """
        # index can be a slice [:] so the return value can be a list
        return self._partial_matches[index]

    def __delitem__(self, index):
        """
        Implements list-style "remove item" semantics.
        """
        del self._partial_matches[index]

    def __iter__(self):
        """
        Implements list-style iteration semantics.
        """
        return iter(self._partial_matches)

    def __contains__(self, item):
        """
        Returns True if the given item is stored and False otherwise.
        """
        return item in self._partial_matches

    def try_clean_expired_partial_matches(self, timestamp: datetime):
        """
        If the number of storage accesses exceeded a predefined threshold, perform a costly operation of removing
        expired partial matches.
        """
        if self._access_count < self._clean_up_interval:
            return
        self._clean_expired_partial_matches(timestamp)
        self._access_count = 0

    def _clean_expired_partial_matches(self, timestamp: datetime):
        """
        Removes partial matches whose earliest timestamp violates the time window constraint.
        """
        if self._sorted_by_arrival_order:
            count = find_partial_match_by_timestamp(self._partial_matches, timestamp)
            self._partial_matches = self._partial_matches[count:]
        else:
            self._partial_matches = list(filter(lambda pm: pm.first_timestamp >= timestamp, self._partial_matches))

    def add(self, pm: PartialMatch):
        """
        Adds a new partial match to the storage.
        """
        raise NotImplementedError()

    def get(self, value: int or float):
        """
        Returns a list of partial matches corresponding to the given value.
        """
        raise NotImplementedError()


class SortedPartialMatchStorage(PartialMatchStorage):
    """
    This class stores the partial matches sorted in increasing order according to a function (key) on partial matches.
    """
    def __init__(self, get_match_key: callable, rel_op: RelopTypes, equation_side: EquationSides,
                 clean_up_interval: int, sort_by_first_timestamp=False, in_leaf=False):
        super().__init__(get_match_key, in_leaf and sort_by_first_timestamp, clean_up_interval)
        self.__get_function = self.__generate_get_function(rel_op, equation_side)

    def __contains__(self, item):
        """
        Returns True if the given item is stored and False otherwise.
        Performs an efficient search in the sorted buffer.
        """
        return item in self.__get_equal(self._get_key(item))

    def add(self, pm: PartialMatch):
        """
        Efficiently inserts the new partial match to the storage according to its key.
        """
        self._access_count += 1
        if self._sorted_by_arrival_order:
            # no need for artificially sorting
            self._partial_matches.append(pm)
            return
        index = get_last_index(self._partial_matches, self._get_key(pm), self._get_key)
        index = 0 if index == -1 else index
        self._partial_matches.insert(index, pm)

    def get(self, value: int or float):
        """
        Applies the storage-specific get() function to extract the required partial matches.
        """
        if len(self._partial_matches) == 0:
            return []
        return self.__get_function(value)

    def __get_equal(self, value: int or float):
        """
        Returns the partial matches whose keys are equal to the given value.
        """
        left_index = get_first_index(self._partial_matches, value, self._get_key)
        if left_index == len(self._partial_matches) or left_index == -1 or \
                self._get_key(self._partial_matches[left_index]) != value:
            return []
        right_index = get_last_index(self._partial_matches, value, self._get_key)
        return self._partial_matches[left_index: right_index + 1]

    def __get_unequal(self, value: int or float):
        """
        Returns the partial matches whose keys are not equal to the given value.
        """
        left_index = get_first_index(self._partial_matches, value, self._get_key)
        if left_index == len(self._partial_matches) or left_index == -1 or \
                self._get_key(self._partial_matches[left_index]) != value:
            return self._partial_matches
        right_index = get_last_index(self._partial_matches, value, self._get_key)
        return self._partial_matches[:left_index] + self._partial_matches[right_index + 1:]

    def __get_greater_aux(self, value: int or float, return_equal: bool):
        """
        An auxiliary method for implementing "greater than" or "greater than or equal to" conditions.
        """
        right_index = get_first_index(self._partial_matches, value, self._get_key) if return_equal \
            else get_last_index(self._partial_matches, value, self._get_key)
        if right_index == len(self._partial_matches):
            return []
        if right_index == -1:
            return self._partial_matches
        # in case value doesn't exist right_index will point on the first one greater than it
        if self._get_key(self._partial_matches[right_index]) != value:
            return self._partial_matches[right_index:]
        return self._partial_matches[right_index:] if return_equal else self._partial_matches[right_index + 1:]

    def __get_greater(self, value: int or float):
        """
        Returns the partial matches whose keys are greater than the given value.
        """
        return self.__get_greater_aux(value, False)

    def __get_greater_or_equal(self, value: int or float):
        """
        Returns the partial matches whose keys are greater than or equal to the given value.
        """
        return self.__get_greater_aux(value, True)

    def __get_smaller_aux(self, value: int or float, return_equal: bool):
        """
        An auxiliary method for implementing "smaller than" or "smaller than or equal to" conditions.
        """
        left_index = get_last_index(self._partial_matches, value, self._get_key) if return_equal \
            else get_first_index(self._partial_matches, value, self._get_key)
        if left_index == len(self._partial_matches):
            return self._partial_matches
        if left_index == -1:
            return []
        # in case value doesn't exist left_index will point on the first one smaller than it
        if self._get_key(self._partial_matches[left_index]) != value:
            return self._partial_matches[: left_index + 1]
        return self._partial_matches[:left_index + 1] if return_equal else self._partial_matches[:left_index]

    def __get_smaller(self, value: int or float):
        """
        Returns the partial matches whose keys are smaller than the given value.
        """
        return self.__get_smaller_aux(value, False)

    def __get_smaller_or_equal(self, value: int or float):
        """
        Returns the partial matches whose keys are smaller than or equal to the given value.
        """
        return self.__get_smaller_aux(value, True)

    def __generate_get_function(self, rel_op: RelopTypes, equation_side: EquationSides):
        """
        Initializes the function responsible for selecting partial matches to be returned upon a get() access.
        """
        if rel_op is None:
            raise Exception("Initialization error: rel_op is None")
        if rel_op == RelopTypes.Equal:
            return self.__get_equal
        if rel_op == RelopTypes.NotEqual:
            return self.__get_unequal

        if rel_op == RelopTypes.Greater:
            return self.__get_greater if equation_side == EquationSides.left else self.__get_smaller
        if rel_op == RelopTypes.Smaller:
            return self.__get_smaller if equation_side == EquationSides.left else self.__get_greater

        if rel_op == RelopTypes.GreaterEqual:
            return self.__get_greater_or_equal if equation_side == EquationSides.left else self.__get_smaller_or_equal
        if rel_op == RelopTypes.SmallerEqual:
            return self.__get_smaller_or_equal if equation_side == EquationSides.left else self.__get_greater_or_equal


class UnsortedPartialMatchStorage(PartialMatchStorage):
    """
    This class stores partial matches unsorted.
    It is used when it's difficult to specify an order that helps when receiving partial matches.
    """
    def __init__(self, clean_up_interval: int, get_match_key: callable = None, in_leaf: bool = False):
        super().__init__(get_match_key if get_match_key is not None else lambda x: x, in_leaf, clean_up_interval)

    def add(self, pm: PartialMatch):
        """
        Appends the given partial match to the match buffer.
        """
        self._access_count += 1
        self._partial_matches.append(pm)

    def get(self, value: int or float):
        """
        Unconditionally returns all the stored matches regardless of the given value.
        """
        return self._partial_matches


class TreeStorageParameters:
    """
    Parameters for the evaluation tree to specify how to store the data.
    """
    def __init__(self, sort_storage: bool = False, attributes_priorities: dict = None,
                 clean_expired_every: int = 100, sort_by_condition_on_attributes: bool = False):
        if sort_storage is None:
            sort_storage = False
        if attributes_priorities is None:
            attributes_priorities = {}
        if clean_expired_every <= 0:
            raise Exception('clean_expired_every should be positive.')
        if sort_by_condition_on_attributes is None:
            sort_by_condition_on_attributes = False
        self.sort_storage = sort_storage
        self.attributes_priorities = attributes_priorities
        self.clean_expired_every = clean_expired_every
        self.sort_by_condition = sort_by_condition_on_attributes
