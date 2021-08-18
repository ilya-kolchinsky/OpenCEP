"""
This file contains various useful functions utilized by different project modules.
"""

from datetime import datetime
from typing import Iterator, Sequence, TypeVar
from typing import List, Container, Optional, Union
from base.Pattern import Pattern
from base.PatternStructure import PrimitiveEventStructure
from itertools import combinations, chain
from base.PatternStructure import SeqOperator
from base.PatternMatch import PatternMatch
from copy import deepcopy
from functools import reduce
from stream.Stream import Stream


def find_partial_match_by_timestamp(partial_matches: List[PatternMatch], timestamp: datetime):
    """
    Returns the partial match from the given list such that its timestamp is the closest to the given timestamp.
    The list is assumed to be sorted according to the earliest event timestamp.
    """
    # should count how many PMs are before last date.
    length = len(partial_matches)
    if length == 0 or partial_matches[0].first_timestamp >= timestamp:
        return 0
    if length == 1:  # here we already know that first item's date < lastDate
        return 1
    if partial_matches[-1].first_timestamp < timestamp:
        return length

    start = 0
    end = length - 1
    while start <= end:
        mid = (start + end) // 2
        mid_timestamp = partial_matches[mid].first_timestamp
        if partial_matches[mid - 1].first_timestamp < timestamp <= mid_timestamp:
            return mid
        elif timestamp > mid_timestamp:
            start = mid + 1
        else:
            end = mid - 1

    # shouldn't get here, because we know not all partial matches are up to date (nor expired),
    # which means an index should be found.
    raise Exception()


def is_float(x: str):
    try:
        _ = float(x)
    except ValueError:
        return False
    else:
        return True


def is_int(x: str) -> bool:
    try:
        _ = int(x)
    except ValueError:
        return False
    else:
        return True


def str_to_number(x: str):
    if is_int(x):
        return int(x)
    if is_float(x):
        return float(x)
    return x


def get_order_by_occurrences(primitive_events: List[PrimitiveEventStructure], occurrences: dict):
    """
    Sorts the given list according to the occurrences dictionary.
    """
    temp_list = [(i, occurrences[primitive_events[i].type]) for i in range(len(primitive_events))]
    temp_list = sorted(temp_list, key=lambda x: x[1])
    return [i[0] for i in temp_list]


def get_all_disjoint_sets(s: frozenset):
    """
    A generator for all disjoint splits of a set.
    """
    if len(s) == 2:
        yield (frozenset({t}) for t in s)
        return

    first = next(iter(s))
    for i in range(len(s) - 1):
        for c in combinations(s.difference({first}), i):
            set1 = frozenset(c).union({first})
            set2 = s.difference(set1)
            yield set1, set2


def merge(arr1: list, arr2: list, key=lambda x: x):
    """
    Merges two lists. The comparison is performed according to the given key function.
    """
    new_len = len(arr1) + len(arr2)
    ret = []
    i = i1 = i2 = 0
    while i < new_len and i1 < len(arr1) and i2 < len(arr2):
        if key(arr1[i1]) < key(arr2[i2]):
            ret.append(arr1[i1])
            i1 += 1
        else:
            ret.append(arr2[i2])
            i2 += 1
        i += 1

    while i1 < len(arr1):
        ret.append(arr1[i1])
        i1 += 1

    while i2 < len(arr2):
        ret.append(arr2[i2])
        i2 += 1

    return ret


def merge_according_to(arr1: list, arr2: list, actual1: list, actual2: list, key: callable = lambda x: x):
    """
    Merge arrays actual1, actual2 according to the way a merge would be done on arr1 and arr2.
    Used in a partial match merge function - the reorders are given, and the partial matches is merged
    according to the reorders.
    """
    if len(arr1) != len(actual1) or len(arr2) != len(actual2):
        raise Exception()

    new_len = len(arr1) + len(arr2)
    ret = []
    i = i1 = i2 = 0
    while i < new_len and i1 < len(arr1) and i2 < len(arr2):
        if key(arr1[i1]) < key(arr2[i2]):
            ret.append(actual1[i1])
            i1 += 1
        else:
            ret.append(actual2[i2])
            i2 += 1
        i += 1

    while i1 < len(arr1):
        ret.append(actual1[i1])
        i1 += 1

    while i2 < len(arr2):
        ret.append(actual2[i2])
        i2 += 1

    return ret


def is_sorted(arr: list, key: callable = lambda x: x, secondary_key: callable = None):
    """
    Returns True if the given list is sorted with respect to the given comparator function and False otherwise.
    """
    if len(arr) == 0:
        return True

    for i in range(len(arr) - 1):
        if key(arr[i]) > key(arr[i + 1]):
            return False
        if secondary_key is not None and secondary_key(arr[i]) > secondary_key(arr[i + 1]):
            return False

    return True


def generate_matches(pattern: Pattern, stream: Stream):
    """
    A recursive, very inefficient pattern match finder.
    It is used as our test creator.
    """
    args = pattern.positive_structure.args
    types = {primitive_event.event_type for primitive_event in args}
    is_seq = (pattern.positive_structure.get_top_operator() == SeqOperator)
    events = {}
    matches = []
    for event in stream:
        if event.event_type in types:
            if event.event_type in events.keys():
                events[event.event_type].append(event)
            else:
                events[event.event_type] = [event]
    generate_matches_recursive(pattern, events, is_seq, [], datetime.max, datetime.min, matches, {})
    return matches


def generate_matches_recursive(pattern: Pattern, events: dict, is_seq: bool, match: list, min_event_timestamp: datetime,
                               max_event_timestamp: datetime,
                               matches: list, binding: dict, loop: int = 0):
    pattern_length = len(pattern.positive_structure.args)
    if loop == pattern_length:
        if pattern.condition.eval(binding):
            if not does_match_exist(matches, match):
                matches.append(PatternMatch(deepcopy(match)))
    else:
        primitive_event = pattern.positive_structure.args[loop]
        for event in events[primitive_event.eventType]:
            min_timestamp = min(min_event_timestamp, event.date)
            max_timestamp = max(max_event_timestamp, event.date)
            binding[primitive_event.name] = event.event
            if max_timestamp - min_timestamp <= pattern.window:
                if not is_seq or len(match) == 0 or match[-1].date <= event.date:
                    match.append(event)
                    generate_matches_recursive(pattern, events, is_seq, match, min_timestamp, max_timestamp, matches,
                                               binding, loop + 1)
                    del match[-1]
        del binding[primitive_event.name]


def does_match_exist(matches: list, match: list):
    """
    Returns True if the given match exists in the list of matches and False otherwise.
    """
    for match2 in matches:
        if len(match) == len(match2.events):
            is_equal = True
            for i in range(len(match)):
                if match[i] != match2.events[i]:
                    is_equal = False
                    break
            if is_equal:
                return True
    return False

T = TypeVar("T")
def powerset_generator(seq: Sequence[T], max_size: int, min_size: int=0) -> Iterator[List[T]]:
    """
    A generator returning all subsets of the given item sequence of size limited to `max_size`
    """
    return map(
        list,
        chain.from_iterable(
            # NOTE: the subsets are ordered by size in descending order to maintain backwards compatability (specifically, some tests depend on it)
            (combinations(seq, i) for i in range(max_size, min_size - 1, -1))
        ),
    )

            
def get_index(container: Container, to_find_value: int, key: callable, return_first_index: bool):
    """
    Returns the index (either the first o the last one depending on the corresponding parameter) of the to_find_value
    in a sequence that's sorted increasingly according to key.
    However in case the to_find_value doesn't exist it returns the index of first value smaller than it or -1.
    """
    start = 0
    end = len(container) - 1
    while start < end:
        mid = (start + end) // 2
        mid_value = key(container[mid])
        if mid_value < to_find_value < key(container[mid + 1]):
            return mid if return_first_index else mid + 1
        if return_first_index:
            if mid_value >= to_find_value:
                end = mid
            else:
                start = mid + 1
        else:
            if mid_value <= to_find_value:
                start = mid + 1
            else:
                end = mid

    if start == end:
        mid_value = key(container[start])
        if mid_value > to_find_value:
            return start - 1
        if mid_value < to_find_value:
            return start + 1
        return start
    return 0


def get_first_index(container: Container, to_find_value: int, key: callable):
    """
    Returns the first instance of the to_find_value in a sequence that's sorted increasingly according to key.
    However in case the to_find_value doesn't exist it returns the index of first value smaller than it or -1.
    """
    return get_index(container, to_find_value, key, True)

    
def get_last_index(container: Container, to_find_value: int, key: callable):
    """
    Returns the last instance of the to_find_value in a sequence that's sorted increasingly according to key.
    However in case the to_find_value doesn't exist it returns the index of first value greater than it or -1.
    """
    return get_index(container, to_find_value, key, False)


def calculate_joint_probability(p1: Optional[float], p2: Optional[float]) -> Optional[float]:
    """
    Calculates the joint probability of two events with given occurrence probabilities p1 and p2.
    If both probabilities come from certain events (i.e., p1=p2=None), None is returned.
    """
    if p1 is None:
        return p2
    if p2 is None:
        return p1
    return p1 * p2


class ndarray:
    """
    Simple implementation of numpy.array
    """

    def __init__(self, array_like):
        if len(array_like) == 0:
            self._data = []
            self.shape = tuple([0])
            self.ndim = 1
            self.size = 0
            return
        elif isinstance(array_like[0], Container):
            self._data = [ndarray(layer) for layer in array_like]
        else:
            self._data = list(array_like)
        if isinstance(self._data[0], ndarray):
            self.shape = tuple([len(self._data)] + list(self._data[0].shape))
            self.ndim = self._data[0].ndim+1
            self.size = self._data[0].size * len(self._data)
        else:
            self.size = len(self._data)
            self.shape = tuple([self.size])
            self.ndim = 1

    def reshape(self, *new_shape: Union[int, Container[int]]):
        if all(isinstance(d, int) for d in new_shape):
            pass
        elif not isinstance(new_shape[0], (list, tuple)) or len(new_shape) != 1:
            raise TypeError
        else:
            new_shape = new_shape[0]
        new_shape = list(new_shape)
        new_size = self._list_mul(new_shape)
        if new_shape.count(-1) > 1:
            raise ValueError("can only specify one unknown dimension")
        if -1 not in new_shape:
            if new_size != self.size:
                raise ValueError(f'cannot reshape array of size {self.size} into shape {tuple(new_shape)}')
        else:
            new_size *= -1
            if new_size > self.size:
                raise ValueError(f'cannot reshape array of size {self.size} into shape {tuple(new_shape)}')
            new_shape[new_shape.index(-1)] = self.size // new_size

        flat_array = self._ndarray_to_1darray()
        if len(new_shape) <= 1:
            return flat_array
        else:
            return flat_array._1darray_to_ndarray(new_shape)

    def __getitem__(self, *slices):
        if all(isinstance(d, int) for d in slices):
            pass
        elif not isinstance(slices[0], Container) or len(slices) != 1:
            raise TypeError
        else:
            slices = slices[0]
        slices = list(slices)
        if len(slices) > self.ndim:
            raise ValueError
        if not slices:
            return self
        elif self.ndim == 1:
            return self._data[slices[0]]
        elif isinstance(slices[0], int):
            return self._data[slices[0]][slices[1:]]
        else:
            return ndarray([layer[slices[1:]] for layer in self._data[slices[0]]])

    def tolist(self):
        if self.ndim == 1:
            return self._data
        else:
            return [layer.tolist() for layer in self._data]

    def __repr__(self):
        return f'array({self.tolist()}, shape={self.shape.__repr__()}'

    def __str__(self):
        return self.tolist().__str__()

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return self.shape[0] if self.shape else 0

    @staticmethod
    def _list_mul(lst):
        return reduce(lambda a, b: a * b, lst) if lst else 0

    def _ndarray_to_1darray(self):
        def matrix_to_list(matrix):
            return [item for sublist in matrix for item in sublist]

        new_list = self._data
        if new_list:
            for _ in range(self.ndim - 1):
                new_list = matrix_to_list(new_list)
        return ndarray(new_list)

    def _1darray_to_ndarray(self, newshape):
        def list_to_matrix(flat_list, inner_dim):
            m = len(flat_list) // inner_dim
            return [[flat_list[i + inner_dim * j] for i in range(inner_dim)] for j in range(m)]

        new_list = self._data
        if len(newshape) > 1:
            for d in newshape[-1:0:-1]:
                new_list = list_to_matrix(new_list, d)
        return ndarray(new_list)

def array(array_like):
    return ndarray(array_like)