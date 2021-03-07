from abc import ABC
from datetime import timedelta, datetime
from queue import Queue
from typing import List, Set, Optional
from dataclasses import dataclass

from base.Event import Event
from condition.Condition import RelopTypes, EquationSides
from condition.CompositeCondition import CompositeCondition, AndCondition
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


@dataclass(frozen=True)
class PatternParameters:
    """
    The parameters of a pattern that are propagated down during evaluation tree during the construction process.
    """
    window: timedelta
    confidence: Optional[float]


class Node(ABC):
    """
    This class represents a single node of an evaluation tree.
    """

    # A static variable specifying whether the system is allowed to delete expired matches.
    # In several very special cases, it is required to switch off this functionality.
    __enable_partial_match_expiration = True

    ###################################### Static methods
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


    ###################################### Initialization
    def __init__(self, pattern_params: PatternParameters, parents, pattern_ids: int or Set[int] = None):
        self._parents = []
        self._sliding_window = pattern_params.window
        self._confidence = pattern_params.confidence
        self._partial_matches = None
        self._condition = AndCondition()

        # Full pattern matches that were not yet reported. Only relevant for an output node, that is, for a node
        # corresponding to a full pattern definition.
        self._unreported_matches = Queue()
        self._is_output_node = False

        # set of event types that will only appear in a single full match
        self._single_event_types = set()
        # events that were added to a partial match and cannot be added again
        self._filtered_events = set()

        # set of pattern IDs with which this node is associated
        if pattern_ids is None:
            pattern_ids = set()
        elif isinstance(pattern_ids, int):
            pattern_ids = {pattern_ids}
        self._pattern_ids = pattern_ids

        # Maps parent to event definition. This field helps to pass the parents a partial match with
        # the right event definitions.
        self._parent_to_info_dict = {}
        # matches that were not yet pushed to the parents for further processing
        self._parent_to_unhandled_queue_dict = {}

        self.set_parents(parents, on_init=True)

    ###################################### Matching-related methods
    def get_next_unreported_match(self):
        """
        Removes and returns an unreported match buffered at this node.
        Used in an output node to collect full pattern matches.
        """
        ret = self._unreported_matches.get()
        return ret

    def has_unreported_matches(self):
        """
        Returns True if this node contains any matches we did not report yet and False otherwise.
        """
        return self._unreported_matches.qsize() > 0

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

    def _add_partial_match(self, pm: PatternMatch):
        """
        Registers a new partial match at this node.
        In case of SortedPatternMatchStorage the insertion is by timestamp or condition, O(log n).
        In case of UnsortedPatternMatchStorage the insertion is directly at the end, O(1).
        """
        self._partial_matches.add(pm)
        for parent in self._parents:
            self._parent_to_unhandled_queue_dict[parent].put(pm)
            parent.handle_new_partial_match(self)
        if self.is_output_node():
            self._unreported_matches.put(pm)

    def __can_add_partial_match(self, pm: PatternMatch) -> bool:
        """
        Returns True if the given partial match can be passed up the tree and False otherwise.
        As of now, checks two things:
        (1) If the stream is probabilistic, validates the confidence threshold;
        (2) If "single" consumption policy is enabled, validates that the partial match contains no already used events.
        """
        if pm.probability is not None:
            # this is a probabilistic stream
            if self._confidence is None:
                raise Exception("Patterns applied on probabilistic event streams must have a confidence threshold")
            if pm.probability < self._confidence:
                # the partial match probability does not match the confidence threshold
                return False
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

    def _validate_and_propagate_partial_match(self, events: List[Event], match_probability: float = None):
        """
        Creates a new partial match from the list of events, validates it, and propagates it up the tree.
        For probabilistic streams, receives the pre-calculated probability of the potential pattern match.
        """
        if not self._validate_new_match(events):
            return
        self._propagate_partial_match(events, match_probability)

    def _propagate_partial_match(self, events: List[Event], match_probability: float = None):
        """
        Receives an already verified list of events for new partial match and propagates it up the tree.
        For probabilistic streams, receives the pre-calculated probability of the potential pattern match.
        """
        new_partial_match = PatternMatch(events, match_probability)
        if self.__can_add_partial_match(new_partial_match):
            self._add_partial_match(new_partial_match)

    def get_partial_matches(self, filter_value: int or float = None):
        """
        Returns only partial matches that can be a good fit the partial match identified by the given filter value.
        """
        return self._partial_matches.get(filter_value) if filter_value is not None \
            else self._partial_matches.get_internal_buffer()

    def _validate_new_match(self, events_for_new_match: List[Event]):
        """
        Validates the condition stored in this node on the given set of events.
        """
        min_timestamp = min([event.timestamp for event in events_for_new_match])
        max_timestamp = max([event.timestamp for event in events_for_new_match])
        return max_timestamp - min_timestamp <= self._sliding_window


    ###################################### Parent- and topology-related methods
    def get_last_unhandled_partial_match_by_parent(self, parent):
        """
        Returns the last partial match buffered at this node and not yet transferred to parent.
        """
        return self._parent_to_unhandled_queue_dict[parent].get(block=False)

    def set_parents(self, parents, on_init: bool = False):
        """
        Sets the parents of this node to the given list of nodes. Providing None as the parameter will render
        this node parentless.
        """
        if parents is None:
            parents = []
        elif isinstance(parents, Node):
            # a single parent was specified
            parents = [parents]
        self._parents = []
        self._parent_to_unhandled_queue_dict = {}
        self._parent_to_info_dict = {}
        for parent in parents:
            self.add_parent(parent, on_init)

    def set_parent(self, parent):
        """
        A more intuitive API for setting the parent list of a node to a single parent.
        Simply invokes set_parents as the latter already supports the case of a single node instead of a list.
        """
        self.set_parents(parent)

    def add_parent(self, parent, on_init: bool = False):
        """
        Adds a parent to this node.
        """
        if parent in self._parents:
            return
        self._parents.append(parent)
        self._parent_to_unhandled_queue_dict[parent] = Queue()
        if not on_init:
            self._parent_to_info_dict[parent] = self.get_positive_event_definitions()

    def get_parents(self):
        """
        Returns the parents of this node.
        """
        return self._parents

    def get_event_definitions_by_parent(self, parent):
        """
        Returns the event definitions according to the parent.
        """
        if parent not in self._parent_to_info_dict.keys():
            raise Exception("parent is not in the dictionary.")
        return self._parent_to_info_dict[parent]


    ###################################### Various setters and getters
    def get_pattern_ids(self):
        """
        Returns the pattern ids of this node.
        """
        return self._pattern_ids

    def get_condition(self):
        """
        Returns the condition of this node.
        """
        return self._condition

    def add_pattern_ids(self, ids: Set[int]):
        """
        Adds a set of Ds of patterns with which this node is associated.
        """
        self._pattern_ids |= ids

    def set_is_output_node(self, is_output_node: bool):
        """
        Sets this node to be defined as an output node according to the given parameter.
        """
        self._is_output_node = is_output_node

    def is_output_node(self):
        """
        Returns whether this node is an output node.
        """
        return self._is_output_node

    def get_storage_unit(self):
        """
        Returns the internal partial match storage of this node.
        """
        return self._partial_matches

    def get_positive_event_definitions(self) -> List[PrimitiveEventDefinition]:
        """
        Returns the specifications of all positive events collected by this tree.
        For non-negative nodes, this method is identical to get_event_definitions()
        """
        return self.get_event_definitions()

    def get_basic_filtering_parameters(self):
        """
        Returns the basic filtering parameters (sliding window and confidence threshold as of now).
        """
        return PatternParameters(self._sliding_window, self._confidence)


    ###################################### Miscellaneous
    def register_single_event_type(self, event_type: str):
        """
        Add the event type to the internal set of event types for which "single" consumption policy is enabled.
        Recursively updates the ancestors of the node.
        """
        self._single_event_types.add(event_type)
        for parent in self._parents:
            parent.register_single_event_type(event_type)

    def apply_condition(self, condition: CompositeCondition):
        """
        Applies the given condition on all nodes in the subtree of this node.
        The process of applying the condition is recursive and proceeds in a bottom-up manner - first the condition is
        propagated down the subtree, then sub-conditions for this node are assigned.
        """
        self._propagate_condition(condition)
        names = {event_def.name for event_def in self.get_event_definitions()}
        self._condition = condition.get_condition_of(names, get_kleene_closure_conditions=False,
                                                     consume_returned_conditions=True)

    def get_first_unbounded_negative_node(self):
        """
        Returns the deepest unbounded node in the subtree of this node.
        It only makes sense to invoke this method from another unbounded negative node because this specific node type
        is always located higher in the tree than any non-negative node. If this call has occurred nevertheless, this is
        an indication of a bug and should thus be treated accordingly.
        """
        raise Exception("get_first_unbounded_negative_node invoked on a non-negative node")

    def set_and_propagate_pattern_parameters(self, pattern_params: PatternParameters):
        """
        Updates and propagates the basic filtering parameters (sliding window and confidence threshold as of now)
        down the subtree of this node.
        Each parameter is only set if it is less restrictive than the currently defined one.
        """
        should_propagate = False
        if pattern_params.window > self._sliding_window:
            should_propagate = True
            self._sliding_window = pattern_params.window
        if self._confidence is not None and \
                (pattern_params.confidence is None or pattern_params.confidence > self._confidence):
            should_propagate = True
            self._confidence = pattern_params.confidence
        if should_propagate:
            self._propagate_pattern_parameters(pattern_params)

    def is_equivalent(self, other):
        """
        Returns True if this node and the given node are equivalent and False otherwise.
        Two nodes are considered equivalent if they possess equivalent structures and the nodes of these structures
        contain equivalent conditions.
        This default implementation only compares the types and conditions of the nodes. The structure equivalence
        test must be implemented by the subclasses
        """
        return type(self) == type(other) and self._condition == other.get_condition()

    ###################################### To be implemented by subclasses

    def propagate_pattern_id(self, pattern_id: int):
        """
        Propagates the given pattern ID down the subtree of this node.
        """
        raise NotImplementedError()

    def create_parent_to_info_dict(self):
        """
        Traverses the subtree of this node and initializes the internal dictionaries mapping each parent node to the
        corresponding event definitions.
        To be implemented by subclasses.
        """
        raise NotImplementedError()

    def get_leaves(self):
        """
        Returns all leaves in this tree - to be implemented by subclasses.
        """
        raise NotImplementedError()

    def _propagate_pattern_parameters(self, pattern_params: PatternParameters):
        """
        Propagates the basic filtering parameters (sliding window and confidence threshold as of now)
        down the subtree of this node.
        """
        raise NotImplementedError()

    def _propagate_condition(self, condition: CompositeCondition):
        """
        Propagates the given condition to successors - to be implemented by subclasses.
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
