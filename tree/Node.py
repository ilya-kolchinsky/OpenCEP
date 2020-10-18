from abc import ABC
from datetime import timedelta, datetime
from queue import Queue
from typing import List

from base.Event import Event
from base.Formula import TrueFormula, Formula, RelopTypes, EquationSides, CompositeFormula
from base.PatternMatch import PatternMatch
from tree.PatternMatchStorage import TreeStorageParameters


class PrimitiveEventDefinition:
    """
    An internal class for capturing the information regarding a single primitive event appearing in a pattern.
    """
    def __init__(self, event_type: str, event_name: str, event_index: int):
        self.type = event_type
        self.name = event_name
        self.index = event_index


class Node(ABC):
    """
    This class represents a single node of an evaluation tree.
    """

    # A static variable specifying whether the system is allowed to delete expired matches.
    # In several very special cases, it is required to switch off this functionality.
    __enable_partial_match_expiration = True

    @staticmethod
    def _toggle_enable_partial_match_expiration(enable):
        """
        Sets the value of the __enable_partial_match_expiration flag.
        """
        Node.__enable_partial_match_expiration = enable

    @staticmethod
    def _is_partial_match_expiration_enabled():
        """
        Returns the static variable specifying whether match expiration is enabled.
        """
        return Node.__enable_partial_match_expiration

    def __init__(self, sliding_window: timedelta, parent):
        self._parent = parent
        self._sliding_window = sliding_window
        self._partial_matches = None
        self._condition = TrueFormula()
        # matches that were not yet pushed to the parent for further processing
        self._unhandled_partial_matches = Queue()
        # set of event types that will only appear in a single full match
        self._single_event_types = set()
        # events that were added to a partial match and cannot be added again
        self._filtered_events = set()

    def consume_first_partial_match(self):
        """
        Removes and returns a single partial match buffered at this node.
        Used in the root node to collect full pattern matches.
        """
        ret = self._partial_matches[0]
        del self._partial_matches[0]
        return ret

    def has_partial_matches(self):
        """
        Returns True if this node contains any partial matches and False otherwise.
        """
        return len(self._partial_matches) > 0

    def get_last_unhandled_partial_match(self):
        """
        Returns the last partial match buffered at this node and not yet transferred to its parent.
        """
        return self._unhandled_partial_matches.get(block=False)

    def set_parent(self, parent):
        """
        Sets the parent of this node.
        """
        self._parent = parent

    def get_root(self):
        """
        Returns the root of the tree.
        """
        node = self
        while node._parent is not None:
            node = node._parent
        return node

    def clean_expired_partial_matches(self, last_timestamp: datetime):
        """
        Removes partial matches whose earliest timestamp violates the time window constraint.
        Also removes the expired filtered events if the "single" consumption policy is enabled.
        """
        if not Node._is_partial_match_expiration_enabled():
            return
        self._partial_matches.try_clean_expired_partial_matches(last_timestamp - self._sliding_window)
        if len(self._single_event_types) == 0:
            # "single" consumption policy is disabled or no event types under the policy reach this node
            return
        self._filtered_events = set([event for event in self._filtered_events
                                    if event.timestamp >= last_timestamp - self._sliding_window])

    def register_single_event_type(self, event_type: str):
        """
        Add the event type to the internal set of event types for which "single" consumption policy is enabled.
        Recursively updates the ancestors of the node.
        """
        self._single_event_types.add(event_type)
        if self._parent is not None:
            self._parent.register_single_event_type(event_type)

    def _add_partial_match(self, pm: PatternMatch):
        """
        Registers a new partial match at this node.
        In case of SortedPatternMatchStorage the insertion is by timestamp or condition, O(log n).
        In case of UnsortedPatternMatchStorage the insertion is directly at the end, O(1).
        """
        self._partial_matches.add(pm)
        if self._parent is not None:
            self._unhandled_partial_matches.put(pm)
            self._parent.handle_new_partial_match(self)

    def __can_add_partial_match(self, pm: PatternMatch) -> bool:
        """
        Returns True if the given partial match can be passed up the tree and False otherwise.
        As of now, only the activation of the "single" consumption policy might prevent this method from returning True.
        In addition, this method updates the filtered events set.
        """
        if len(self._single_event_types) == 0:
            return True
        new_filtered_events = set()
        for event in pm.events:
            if event.type not in self._single_event_types:
                continue
            if event in self._filtered_events:
                # this event was already passed
                return False
            else:
                # this event was not yet passed but should only be passed once - remember it
                new_filtered_events.add(event)
        self._filtered_events |= new_filtered_events
        return True

    def _validate_and_propagate_partial_match(self, events: List[Event]):
        """
        Creates a new partial match from the list of events, validates it, and propagates it up the tree.
        """
        if not self._validate_new_match(events):
            return
        self._propagate_partial_match(events)

    def _propagate_partial_match(self, events: List[Event]):
        """
        Receives an already verified list of events for new partial match and propagates it up the tree.
        """
        new_partial_match = PatternMatch(events)
        if self.__can_add_partial_match(new_partial_match):
            self._add_partial_match(new_partial_match)

    def get_partial_matches(self, filter_value: int or float = None):
        """
        Returns only partial matches that can be a good fit the partial match identified by the given filter value.
        """
        return self._partial_matches.get(filter_value) if filter_value is not None \
            else self._partial_matches.get_internal_buffer()

    def get_storage_unit(self):
        """
        Returns the internal partial match storage of this node.
        """
        return self._partial_matches

    def _validate_new_match(self, events_for_new_match: List[Event]):
        """
        Validates the condition stored in this node on the given set of events.
        """
        min_timestamp = min([event.timestamp for event in events_for_new_match])
        max_timestamp = max([event.timestamp for event in events_for_new_match])
        return max_timestamp - min_timestamp <= self._sliding_window

    def get_leaves(self):
        """
        Returns all leaves in this tree - to be implemented by subclasses.
        """
        raise NotImplementedError()

    def apply_formula(self, formula: Formula, ignore_kc=True):
        """
        Applies a given formula on all nodes in this tree - to be implemented by subclasses.
        """
        self._propagate_condition(formula)
        self._assign_formula(formula, ignore_kc)
        if isinstance(formula, CompositeFormula):
            self._consume_formula(formula, ignore_kc)

    def _propagate_condition(self, formula: Formula):
        """
        Propagation method to successors.
        """
        raise NotImplementedError()

    def _assign_formula(self, formula: Formula, ignore_kc):
        """
        Formula assign method to current node. Should assign a Formula to self._condition.
        """
        raise NotImplementedError()

    def _consume_formula(self, formula: Formula, ignore_kc):
        """
        Formula consumption method. Should consume the formulas assigned to self._condition after _assign_formulas.
        :param formula: input formula to consume formulas from.
        :param ignore_kc: True to ignore KCFormulas, False to get them, and only them.
        :return:
        """
        raise NotImplementedError()

    def get_event_definitions(self) -> List[PrimitiveEventDefinition]:
        """
        Returns the specifications of all events collected by this tree - to be implemented by subclasses.
        """
        raise NotImplementedError()

    def get_structure_summary(self):
        """
        Returns the summary of the subtree rooted at this node - to be implemented by subclasses.
        """
        raise NotImplementedError()

    def create_storage_unit(self, storage_params: TreeStorageParameters, sorting_key: callable = None,
                            rel_op: RelopTypes = None, equation_side: EquationSides = None,
                            sort_by_first_timestamp: bool = False):
        """
        An abstract method for recursive partial match storage initialization.
        """
        raise NotImplementedError()