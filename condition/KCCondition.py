"""
This file contains the Kleene closure condition classes.
"""
from abc import ABC

from condition.Condition import AtomicCondition


class KCCondition(AtomicCondition, ABC):
    """
    The base class for conditions operating on Kleene closure matches.
    """
    def __init__(self, names: set, getattr_func: callable, relation_op: callable):
        self._names = names
        self._getattr_func = getattr_func
        self._relation_op = relation_op

    def is_condition_of(self, names: set):
        if names == self._names:
            return True
        if len(names) != len(self._names):
            return False
        for name in names:
            if not any([name in n for n in self._names]):
                return False
        return True

    @staticmethod
    def _validate_index(index: int, lst: list):
        """
        Validates that the given index is within the bounds of the given list.
        """
        return 0 <= index < len(lst)

    def get_event_names(self):
        """
        Returns the event names associated with this condition.
        """
        return self._names

    def __repr__(self):
        return "KC [" + ", ".join(self._names) + "]"

    def __eq__(self, other):
        return self == other or (type(self) == type(other) and
                                 self._names == other._names and
                                 self._getattr_func == other._getattr_func and
                                 self._relation_op == other._relation_op)


class KCIndexCondition(KCCondition):
    """
    This class represents KCConditions that perform operations between 2 indexes of the KleeneClosure events.
    It supports comparisons of 2 types:
        - first_index and second_index will compare 2 specific indexes from the KC events
        - offset will compare every 2 items in KC events that meet the offset requirement. Supports negative offsets.

    If the offset is larger than the length of the list for offset mechanism,
        or 1 of the indexes is negative or out of bounds for index mechanism,
        the condition returns False.
    """
    def __init__(self, names: set, getattr_func: callable, relation_op: callable,
                 first_index=None, second_index=None, offset=None):
        """
        Enforce getting 1 of 2 activations types ONLY:
            1) first_index and second_index to compare
            2) offset to compare every 2 items that meet offset requirement (either positive or negative)
        Further activation types may be implemented for convenience.
        """
        if not self.__validate_params(first_index, second_index, offset):
            raise Exception("Invalid use of KCIndex condition.\nboth index and offset are not None\n refer to comment")
        super().__init__(names, getattr_func, relation_op)
        self.__first_index = first_index
        self.__second_index = second_index
        self.__offset = offset

    def _eval(self, event_list: list = None):
        # offset is active - choose evaluation by offset mechanism
        if self.__offset is not None:
            return self.__eval_by_offset(event_list)
        # offset is not active - choose evaluation by index mechanism
        return self.__eval_by_index(event_list)

    def __eval_by_index(self, event_list: list):
        """
        Handles the evaluation of an index-based condition.
        """
        # validate both indexes
        if not self._validate_index(self.__first_index, event_list) or \
                not self._validate_index(self.__second_index, event_list):
            return False
        # get the items
        item_1 = event_list[self.__first_index]
        item_2 = event_list[self.__second_index]
        # execute the relation op on both items
        if not self._relation_op(self._getattr_func(item_1), self._getattr_func(item_2)):
            return False
        return True

    def __eval_by_offset(self, event_list: list):
        """
        Handles the evaluation of an offset-based condition.
        This can be a very time-consuming process for large power-sets.
        """
        # offset too large restriction
        if self.__offset >= len(event_list):
            return False

        for i in range(len(event_list)):
            # test if i + offset meets index requirements ( 0 <= i <= len(event_list) - 1)
            if not self._validate_index(i + self.__offset, event_list):
                continue
            # use AND logic to return True if EVERY two items that meet offset requirement return True. (early Abort)
            if not self._relation_op(self._getattr_func(event_list[i]),
                                     self._getattr_func(event_list[i + self.__offset])):
                return False
        return True

    @staticmethod
    def __validate_params(first_index, second_index, offset):
        """
        Current supported patterns allow (first_index AND second_index) OR (offset) AND (NOT BOTH).
        Disqualification semantics used to allow easier extensions in the future - simply remove the newly supported
        patterns from the disqualified patterns.
        """
        return not (                                                                              # idx1 idx2 offset
                (first_index is None and second_index is None and offset is None) or              # 0     0     0
                (first_index is not None and second_index is not None and offset is not None) or  # 1     1     1
                (first_index is not None and offset is not None) or                               # 1     0     1
                (second_index is not None and offset is not None) or                              # 0     1     1
                (first_index is None and second_index is not None) or                             # 1     0     0
                (first_index is not None and second_index is None)                                # 0     1     0
        )

    def get_first_index(self):
        return self.__first_index

    def get_second_index(self):
        return self.__second_index

    def get_offset(self):
        return self.__offset

    def __repr__(self):
        if self.__first_index is not None:
            return "KCIndex first_index={}, second_index={} [".format(self.__first_index, self.__second_index) + \
                   ", ".join(self._names) + "]"
        else:
            return "KCIndex offset={} [".format(self.__offset) + ", ".join(self._names) + "]"

    def __eq__(self, other):
        return super().__eq__(other) and self.__first_index == other.get_first_index() and \
               self.__second_index == other.get_second_index() and self.__offset == other.get_offset()


class KCValueCondition(KCCondition):
    """
    This class represents KCConditions that perform operations between events from the KleeneClosure events
    and an arbitrary value.
    It supports comparisons of 2 types:
        - value only comparison will compare all the items in KC events to a specific value
        - value and index comparison will compare a specific index from KC events to a specific value
    """
    def __init__(self, names: set, getattr_func: callable, relation_op: callable, value, index: int = None):
        super().__init__(names, getattr_func, relation_op)
        self.__value = value
        self.__index = index

    def _eval(self, event_list: list = None):
        # index comparison method and invalid index - Abort.
        if self.__index is not None and not self._validate_index(self.__index, event_list):
            return False

        if self.__index is None:
            # no index used for comparison - compare all elements
            for item in event_list:
                # use AND logic to return True if EVERY item returns True when being compared to condition's value.
                if not self._relation_op(self._getattr_func(item), self.__value):
                    return False
        else:
            # compare 1 element from the list of events to the condition's value
            if not self._relation_op(self._getattr_func(event_list[self.__index]), self.__value):
                return False
        return True

    def get_value(self):
        return self.__value

    def get_index(self):
        return self.__index

    def __repr__(self):
        return "KCValue, index={}, value={} [".format(self.__index, self.__value) + ", ".join(self._names) + "]"

    def __eq__(self, other):
        return super().__eq__(other) and self.__value == other.get_value() and self.__index == other.get_index()
