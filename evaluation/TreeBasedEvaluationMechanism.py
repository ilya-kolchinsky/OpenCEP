from queue import Queue
from datetime import timedelta
from base.Event import Event
from misc.Utils import *
from base.Formula import TrueFormula, Formula, RelopTypes, EquationSides, IdentifierTerm, BinaryFormula, CompositeFormula, KCFormula
from evaluation.PartialMatchStorage import SortedPartialMatchStorage, UnsortedPartialMatchStorage, TreeStorageParameters
from typing import Tuple, Dict
from base.PatternMatch import PatternMatch
from base.PatternStructure import *
from evaluation.EvaluationMechanism import EvaluationMechanism
from misc.ConsumptionPolicy import *

import time


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
        return self._unhandled_partial_matches.get()

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

    def _add_partial_match(self, pm: PartialMatch):
        """
        Registers a new partial match at this node.
        In case of SortedPartialMatchStorage the insertion is by timestamp or condition, O(log n).
        In case of UnsortedPartialMatchStorage the insertion is directly at the end, O(1).
        """
        self._partial_matches.add(pm)
        if self._parent is not None:
            self._unhandled_partial_matches.put(pm)
            self._parent.handle_new_partial_match(self)

    def __can_add_partial_match(self, pm: PartialMatch) -> bool:
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
        new_partial_match = PartialMatch(events)
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

    def apply_formula(self, formula: Formula):
        """
        Applies a given formula on all nodes in this tree - to be implemented by subclasses.
        """
        kc_nodes = []
        kc_nodes.extend(self._propagate_condition(formula))
        if isinstance(self, KleeneClosureNode):
            # save a reference to current KC node for future use
            kc_nodes.append(self)
        else:
            self._assign_formula(formula)
            if isinstance(formula, CompositeFormula):
                self._consume_formula(formula)
        return kc_nodes

    def _propagate_condition(self, formula: Formula):
        raise NotImplementedError()

    def _assign_formula(self, formula: Formula):
        raise NotImplementedError()

    def _consume_formula(self, formula: Formula):
        raise NotImplementedError()

    def get_event_definitions(self):
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


class LeafNode(Node):
    """
    A leaf node is responsible for a single event type of the pattern.
    """
    def __init__(self, sliding_window: timedelta, leaf_index: int, leaf_qitem: QItem, parent: Node):
        super().__init__(sliding_window, parent)
        self.__leaf_index = leaf_index
        self.__event_name = leaf_qitem.name
        self.__event_type = leaf_qitem.type

    def get_leaves(self):
        return [self]

    def get_event_definitions(self):
        return [(self.__leaf_index, QItem(self.__event_type, self.__event_name))]

    def get_event_type(self):
        """
        Returns the type of events processed by this leaf.
        """
        return self.__event_type

    def get_event_name(self):
        """
        Returns the name of events processed by this leaf.
        """
        return self.__event_name

    def get_leaf_index(self):
        """
        Returns the index of this leaf.
        """
        return self.__leaf_index

    def set_leaf_index(self, index: int):
        """
        Sets the index of this leaf.
        """
        self.__leaf_index = index

    def handle_event(self, event: Event):
        """
        Inserts the given event to this leaf.
        """
        self.clean_expired_partial_matches(event.timestamp)
        self._validate_and_propagate_partial_match([event])

    def _validate_new_match(self, events_for_new_match: List[Event]):
        """
        Validates the condition stored in this node on the given set of events.
        """
        if not super()._validate_new_match(events_for_new_match):
            return False
        binding = {self.__event_name: events_for_new_match[0].payload}
        return self._condition.eval(binding)

    def _propagate_condition(self, formula: Formula):
        return []

    def _assign_formula(self, formula: Formula):
        condition = formula.get_formula_of(self.__event_name)
        if condition:
            self._condition = condition

    def _consume_formula(self, formula: Formula):
        formula.consume_formula_of(self.__event_name)

    def create_storage_unit(self, storage_params: TreeStorageParameters, sorting_key: callable = None,
                            rel_op: RelopTypes = None, equation_side: EquationSides = None,
                            sort_by_first_timestamp: bool = False):
        """
        For leaf nodes, we always want to create a sorted storage, since the events arrive in their natural order
        of occurrence anyway. Hence, a sorted storage is initialized either according to a user-specified key, or an
        arrival order if no storage parameters were explicitly specified.
        """
        should_use_default_storage_mode = not storage_params.sort_storage or sorting_key is None
        actual_sorting_key = (lambda pm: pm.events[0].timestamp) if should_use_default_storage_mode else sorting_key
        actual_sort_by_first_timestamp = should_use_default_storage_mode or sort_by_first_timestamp
        self._partial_matches = SortedPartialMatchStorage(actual_sorting_key, rel_op, equation_side,
                                                          storage_params.clean_up_interval,
                                                          actual_sort_by_first_timestamp, True)

    def get_structure_summary(self):
        return self.__event_name


class InternalNode(Node, ABC):
    """
    This class represents a non-leaf node of an evaluation tree.
    """
    def __init__(self, sliding_window: timedelta, parent: Node = None, event_defs: List[Tuple[int, QItem]] = None):
        super().__init__(sliding_window, parent)
        self._event_defs = event_defs

    def get_event_definitions(self):
        return self._event_defs

    def _validate_new_match(self, events_for_new_match: List[Event]):
        """
        Validates the condition stored in this node on the given set of events.
        """
        if not super()._validate_new_match(events_for_new_match):
            return False
        binding = {
            self._event_defs[i][1].name: events_for_new_match[i].payload for i in range(len(self._event_defs))
        }
        return self._condition.eval(binding)

    def _propagate_condition(self, condition: Formula):
        """
        Propagates the given condition to the sub tree(s).
        """
        raise NotImplementedError()

    def _assign_formula(self, formula: Formula):
        names = {item[1].name for item in self._event_defs}
        condition = formula.get_formula_of(names)
        self._condition = condition if condition else TrueFormula()

    def _consume_formula(self, formula: Formula):
        names = {item[1].name for item in self._event_defs}
        formula.consume_formula_of(names)

    def _init_storage_unit(self, storage_params: TreeStorageParameters, sorting_key: callable = None,
                           rel_op: RelopTypes = None, equation_side: EquationSides = None,
                           sort_by_first_timestamp: bool = False):
        """
        An auxiliary method for setting up the storage of an internal node.
        In the internal nodes, we only sort the storage if a storage key is explicitly provided by the user.
        """
        if not storage_params.sort_storage or sorting_key is None:
            self._partial_matches = UnsortedPartialMatchStorage(storage_params.clean_up_interval)
        else:
            self._partial_matches = SortedPartialMatchStorage(sorting_key, rel_op, equation_side,
                                                              storage_params.clean_up_interval, sort_by_first_timestamp)

    def handle_new_partial_match(self, partial_match_source: Node):
        """
        A handler for a notification regarding a new partial match generated at one of this node's children.
        """
        raise NotImplementedError()


class UnaryNode(InternalNode, ABC):
    """
    Represents an internal tree node with a single child.
    """
    def __init__(self, sliding_window: timedelta, parent: Node = None, event_defs: List[Tuple[int, QItem]] = None,
                 child: Node = None):
        super().__init__(sliding_window, parent, event_defs)
        self._child = child

    def get_leaves(self):
        if self._child is None:
            raise Exception("Unary Node with no child")
        return self._child.get_leaves()

    def _propagate_condition(self, condition: Formula):
        return self._child.apply_formula(condition)

    def set_subtree(self, child: Node):
        """
        Sets the child node of this node.
        """
        self._child = child
        self._event_defs = child.get_event_definitions()

    def create_storage_unit(self, storage_params: TreeStorageParameters, sorting_key: callable = None,
                            rel_op: RelopTypes = None, equation_side: EquationSides = None,
                            sort_by_first_timestamp: bool = False):
        self._init_storage_unit(storage_params, sorting_key, rel_op, equation_side)
        self._child.create_storage_unit(storage_params)


class BinaryNode(InternalNode, ABC):
    """
    An internal node connects two subtrees, i.e., two subpatterns of the evaluated pattern.
    """
    def __init__(self, sliding_window: timedelta, parent: Node = None, event_defs: List[Tuple[int, QItem]] = None,
                 left: Node = None, right: Node = None):
        super().__init__(sliding_window, parent, event_defs)
        self._left_subtree = left
        self._right_subtree = right

    def get_leaves(self):
        result = []
        if self._left_subtree is not None:
            result += self._left_subtree.get_leaves()
        if self._right_subtree is not None:
            result += self._right_subtree.get_leaves()
        return result

    def _propagate_condition(self, condition: Formula):
        return self._left_subtree.apply_formula(condition) + self._right_subtree.apply_formula(condition)

    def _set_event_definitions(self,
                               left_event_defs: List[Tuple[int, QItem]], right_event_defs: List[Tuple[int, QItem]]):
        """
        A helper function for collecting the event definitions from subtrees.
        """
        self._event_defs = left_event_defs + right_event_defs

    def get_left_subtree(self):
        """
        Returns the left subtree of this node.
        """
        return self._left_subtree

    def get_right_subtree(self):
        """
        Returns the right subtree of this node.
        """
        return self._right_subtree

    def set_subtrees(self, left: Node, right: Node):
        """
        Sets the subtrees of this node.
        """
        self._left_subtree = left
        self._right_subtree = right
        self._set_event_definitions(self._left_subtree.get_event_definitions(),
                                    self._right_subtree.get_event_definitions())

    def handle_new_partial_match(self, partial_match_source: Node):
        """
        Internal node's update for a new partial match in one of the subtrees.
        """
        if partial_match_source == self._left_subtree:
            other_subtree = self._right_subtree
        elif partial_match_source == self._right_subtree:
            other_subtree = self._left_subtree
        else:
            raise Exception()  # should never happen

        new_partial_match = partial_match_source.get_last_unhandled_partial_match()
        new_pm_key = partial_match_source.get_storage_unit().get_key_function()
        first_event_defs = partial_match_source.get_event_definitions()
        other_subtree.clean_expired_partial_matches(new_partial_match.last_timestamp)
        partial_matches_to_compare = other_subtree.get_partial_matches(new_pm_key(new_partial_match))
        second_event_defs = other_subtree.get_event_definitions()

        if self._parent is not None:
            self.clean_expired_partial_matches(new_partial_match.last_timestamp)

        # given a partial match from one subtree, for each partial match
        # in the other subtree we check for new partial matches in this node.
        self._try_create_new_matches(new_partial_match, partial_matches_to_compare, first_event_defs, second_event_defs)

    def _try_create_new_matches(self, new_partial_match: PartialMatch, partial_matches_to_compare: List[PartialMatch],
                                first_event_defs: List[Tuple[int, QItem]], second_event_defs: List[Tuple[int, QItem]]):
        """
        For each candidate pair of partial matches that can be joined to create a new one, verifies all the
        necessary conditions creates new partial matches if all constraints are satisfied.
        """
        for partial_match in partial_matches_to_compare:
            events_for_new_match = self._merge_events_for_new_match(first_event_defs, second_event_defs,
                                                                    new_partial_match.events, partial_match.events)
            self._validate_and_propagate_partial_match(events_for_new_match)

    def _merge_events_for_new_match(self,
                                    first_event_defs: List[Tuple[int, QItem]],
                                    second_event_defs: List[Tuple[int, QItem]],
                                    first_event_list: List[Event],
                                    second_event_list: List[Event]):
        """
        Creates a list of events to be included in a new partial match.
        """
        if self._event_defs[0][0] == first_event_defs[0][0]:
            return first_event_list + second_event_list
        if self._event_defs[0][0] == second_event_defs[0][0]:
            return second_event_list + first_event_list
        raise Exception()

    def __get_filtered_conditions(self, left_event_names: List[str], right_event_names: List[str]):
        """
        An auxiliary method returning the atomic conditions containing variables from the opposite subtrees of this
        internal node.
        """
        # Note that as of now self._condition contains the wrong values for most nodes - to be fixed in future
        atomic_conditions = self._condition.extract_atomic_formulas()
        filtered_conditions = []
        for atomic_condition in atomic_conditions:
            if not isinstance(atomic_condition.left_term, IdentifierTerm):
                continue
            if not isinstance(atomic_condition.right_term, IdentifierTerm):
                continue
            if atomic_condition.left_term.name in left_event_names and \
                    atomic_condition.right_term.name in right_event_names:
                filtered_conditions.append(atomic_condition)
            elif atomic_condition.right_term.name in left_event_names and \
                    atomic_condition.left_term.name in right_event_names:
                filtered_conditions.append(atomic_condition)
        return filtered_conditions

    def __get_params_for_sorting_keys(self, conditions: List[BinaryFormula], attributes_priorities: dict,
                                      left_event_names: List[str], right_event_names: List[str]):
        """
        An auxiliary method returning the best assignments for the parameters of the sorting keys according to the
        available atomic conditions and user-supplied attribute priorities.
        """
        left_term, left_rel_op, left_equation_size = None, None, None
        right_term, right_rel_op, right_equation_size = None, None, None
        for condition in conditions:
            if condition.left_term.name in left_event_names:
                if left_term is None or attributes_priorities[condition.left_term.name] > \
                        attributes_priorities[left_term.name]:
                    left_term, left_rel_op, left_equation_size = \
                        condition.left_term, condition.get_relop(), EquationSides.left
                if right_term is None or attributes_priorities[condition.right_term.name] > \
                        attributes_priorities[right_term.name]:
                    right_term, right_rel_op, right_equation_size = \
                        condition.right_term, condition.get_relop(), EquationSides.right
            elif condition.left_term.name in right_event_names:
                if left_term is None or attributes_priorities[condition.right_term.name] > \
                        attributes_priorities[left_term.name]:
                    left_term, left_rel_op, left_equation_size = \
                        condition.right_term, condition.get_relop(), EquationSides.right
                if right_term is None or attributes_priorities[condition.left_term.name] > \
                        attributes_priorities[right_term.name]:
                    right_term, right_rel_op, right_equation_size = \
                        condition.left_term, condition.get_relop(), EquationSides.left
            else:
                raise Exception("Internal error")
        return left_term, left_rel_op, left_equation_size, right_term, right_rel_op, right_equation_size

    def _get_condition_based_sorting_keys(self, attributes_priorities: dict):
        """
        Calculates the sorting keys according to the conditions in the pattern and the user-provided priorities.
        """
        left_sorting_key, right_sorting_key, rel_op = None, None, None
        left_event_defs = self._left_subtree.get_event_definitions()
        right_event_defs = self._right_subtree.get_event_definitions()
        left_event_names = {item[1].name for item in left_event_defs}
        right_event_names = {item[1].name for item in right_event_defs}

        # get the candidate atomic conditions
        filtered_conditions = self.__get_filtered_conditions(left_event_names, right_event_names)
        if len(filtered_conditions) == 0:
            # no conditions to sort according to
            return None, None, None, None, None, None
        if attributes_priorities is None and len(filtered_conditions) > 1:
            # multiple conditions are available, yet the user did not provide a list of priorities
            return None, None, None, None, None, None

        # select the most fitting atomic conditions and assign the respective parameters
        left_term, left_rel_op, left_equation_size, right_term, right_rel_op, right_equation_size = \
            self.__get_params_for_sorting_keys(filtered_conditions, attributes_priorities,
                                               left_event_names, right_event_names)

        # convert terms into sorting key fetching callbacks
        if left_term is not None:
            left_sorting_key = lambda pm: left_term.eval(
                {left_event_defs[i][1].name: pm.events[i].payload for i in range(len(pm.events))}
            )
        if right_term is not None:
            right_sorting_key = lambda pm: right_term.eval(
                {right_event_defs[i][1].name: pm.events[i].payload for i in range(len(pm.events))}
            )

        return left_sorting_key, left_rel_op, left_equation_size, right_sorting_key, right_rel_op, right_equation_size


class AndNode(BinaryNode):
    """
    An internal node representing an "AND" operator.
    """
    def get_structure_summary(self):
        return ("And",
                self._left_subtree.get_structure_summary(),
                self._right_subtree.get_structure_summary())

    def create_storage_unit(self, storage_params: TreeStorageParameters, sorting_key: callable = None,
                            rel_op: RelopTypes = None, equation_side: EquationSides = None,
                            sort_by_first_timestamp: bool = False):
        self._init_storage_unit(storage_params, sorting_key, rel_op, equation_side)
        if not storage_params.sort_storage:
            # efficient storage is disabled
            self._left_subtree.create_storage_unit(storage_params)
            self._right_subtree.create_storage_unit(storage_params)
            return
        # efficient storage was explicitly enabled
        left_key, left_rel_op, left_equation_size, right_key, right_rel_op, right_equation_size = \
            self._get_condition_based_sorting_keys(storage_params.attributes_priorities)
        self._left_subtree.create_storage_unit(storage_params, left_key, left_rel_op, left_equation_size)
        self._right_subtree.create_storage_unit(storage_params, right_key, right_rel_op, right_equation_size)


class SeqNode(BinaryNode):
    """
    An internal node representing a "SEQ" (sequence) operator.
    In addition to checking the time window and condition like the basic node does, SeqNode also verifies the order
    of arrival of the events in the partial matches it constructs.
    """
    def _set_event_definitions(self,
                               left_event_defs: List[Tuple[int, QItem]], right_event_defs: List[Tuple[int, QItem]]):
        self._event_defs = merge(left_event_defs, right_event_defs, key=lambda x: x[0])

    def _merge_events_for_new_match(self,
                                    first_event_defs: List[Tuple[int, QItem]],
                                    second_event_defs: List[Tuple[int, QItem]],
                                    first_event_list: List[Event],
                                    second_event_list: List[Event]):
        return merge_according_to(first_event_defs, second_event_defs,
                                  first_event_list, second_event_list, key=lambda x: x[0])

    def _validate_new_match(self, events_for_new_match: List[Event]):
        if not is_sorted(events_for_new_match, key=lambda x: x.timestamp):
            return False
        return super()._validate_new_match(events_for_new_match)

    def get_structure_summary(self):
        return ("Seq",
                self._left_subtree.get_structure_summary(),
                self._right_subtree.get_structure_summary())

    def _get_sequence_based_sorting_keys(self):
        """
        Calculates the sorting keys according to the pattern sequence order and the user-provided priorities.
        """
        left_event_defs = self._left_subtree.get_event_definitions()
        right_event_defs = self._right_subtree.get_event_definitions()
        # comparing min and max leaf index of two subtrees
        min_left = min(left_event_defs, key=lambda x: x[0])[0]  # [ { ] } or [ { } ]
        max_left = max(left_event_defs, key=lambda x: x[0])[0]  # { [ } ] or { [ ] }
        min_right = min(right_event_defs, key=lambda x: x[0])[0]  # [ ] { }
        max_right = max(right_event_defs, key=lambda x: x[0])[0]  # { } [ ]
        if max_left < min_right:  # 3)
            left_sort, right_sort, rel_op = -1, 0, RelopTypes.SmallerEqual
        elif max_right < min_left:  # 4)
            left_sort, right_sort, rel_op = 0, -1, RelopTypes.GreaterEqual
        elif min_left < min_right:  # 1)
            left_sort, right_sort, rel_op = 0, 0, RelopTypes.SmallerEqual
        elif min_right < min_left:  # 2)
            left_sort, right_sort, rel_op = 0, 0, RelopTypes.GreaterEqual
        if rel_op is None:
            raise Exception("rel_op is None, something bad has happened")
        left_sorting_key = lambda pm: pm.events[left_sort].timestamp
        right_sorting_key = lambda pm: pm.events[right_sort].timestamp
        # left/right_sort == 0 means that left/right subtree will be sorted by first timestamp
        return left_sorting_key, right_sorting_key, rel_op, (left_sort == 0), (right_sort == 0)

    def create_storage_unit(self, storage_params: TreeStorageParameters, sorting_key: callable = None,
                            rel_op: RelopTypes = None, equation_side: EquationSides = None,
                            sort_by_first_timestamp: bool = False):
        """
        This function creates the storage for partial_matches it gives a special key: callable
        to the storage unit which tells the storage unit on which attribute(only timestamps here)
        to sort.
        We assume all events are in SEQ(,,,,...) which makes the order in partial match the same
        as in event_defs: [(1,a),(2,b)] in event_defs and [a,b] in pm.
        """
        self._init_storage_unit(storage_params, sorting_key, rel_op, equation_side, sort_by_first_timestamp)
        if not storage_params.sort_storage:
            # efficient storage is disabled
            self._left_subtree.create_storage_unit(storage_params)
            self._right_subtree.create_storage_unit(storage_params)
            return
        left_sort_by_first_timestamp, right_sort_by_first_timestamp = False, False
        # finding sorting keys in case user requested to sort by condition
        if storage_params.prioritize_sorting_by_timestamp:
            # first try the timestamps, then the conditions
            left_key, right_key, left_rel_op, left_sort_by_first_timestamp, right_sort_by_first_timestamp = \
                self._get_sequence_based_sorting_keys()
            if left_rel_op is None:
                left_key, left_rel_op, left_equation_size, right_key, right_rel_op, right_equation_size = \
                    self._get_condition_based_sorting_keys(storage_params.attributes_priorities)
            else:
                right_rel_op, left_equation_size, right_equation_size = \
                    left_rel_op, EquationSides.left, EquationSides.right
        else:
            # first try the conditions, then the timestamps
            left_key, left_rel_op, left_equation_size, right_key, right_rel_op, right_equation_size = \
                self._get_condition_based_sorting_keys(storage_params.attributes_priorities)
            if left_rel_op is None:
                left_key, right_key, left_rel_op, left_sort_by_first_timestamp, right_sort_by_first_timestamp = \
                    self._get_sequence_based_sorting_keys()
                right_rel_op, left_equation_size, right_equation_size = \
                    left_rel_op, EquationSides.left, EquationSides.right
        if left_rel_op is None:
            # both sequence-based and condition-based initialization failed
            raise Exception("Should never happen")
        self._left_subtree.create_storage_unit(storage_params, left_key, left_rel_op, left_equation_size,
                                               left_sort_by_first_timestamp)
        self._right_subtree.create_storage_unit(storage_params, right_key, right_rel_op, right_equation_size,
                                                right_sort_by_first_timestamp)


class NegationNode(BinaryNode, ABC):
    """
    An internal node representing a negation operator.
    This implementation heavily relies on the fact that, if any unbounded negation operators are defined in the
    pattern, they are conveniently placed at the top of the tree forming a left-deep chain of nodes.
    """
    def __init__(self, sliding_window: timedelta, is_unbounded: bool, top_operator, parent: Node = None,
                 event_defs: List[Tuple[int, QItem]] = None,
                 left: Node = None, right: Node = None):
        super().__init__(sliding_window, parent, event_defs, left, right)

        # aliases for the negative node subtrees to make the code more readable
        # by construction, we always have the positive subtree on the left
        self._positive_subtree = self._left_subtree
        self._negative_subtree = self._right_subtree

        # negation operators that can appear in the end of the match have this flag on
        self.__is_unbounded = is_unbounded

        # the multinary operator of the root node
        self.__top_operator = top_operator

        # a list of partial matches that can be invalidated by a negative event that will only arrive in future
        self.__pending_partial_matches = []

    def set_subtrees(self, left: Node, right: Node):
        """
        Updates the aliases following the changes in the subtrees.
        """
        super().set_subtrees(left, right)
        self._positive_subtree = self._left_subtree
        self._negative_subtree = self._right_subtree

    def clean_expired_partial_matches(self, last_timestamp: datetime):
        """
        In addition to the normal functionality of this method, attempt to flush pending matches that can already
        be propagated.
        """
        super().clean_expired_partial_matches(last_timestamp)
        if self.__is_first_unbounded_negative_node():
            self.flush_pending_matches(last_timestamp)

    def flush_pending_matches(self, last_timestamp: datetime = None):
        """
        Releases the partial matches in the pending matches buffer. If the timestamp is provided, only releases
        expired matches.
        """
        if last_timestamp is not None:
            self.__pending_partial_matches = sorted(self.__pending_partial_matches, key=lambda x: x.first_timestamp)
            count = find_partial_match_by_timestamp(self.__pending_partial_matches,
                                                    last_timestamp - self._sliding_window)
            matches_to_flush = self.__pending_partial_matches[:count]
            self.__pending_partial_matches = self.__pending_partial_matches[count:]
        else:
            matches_to_flush = self.__pending_partial_matches

        # since matches_to_flush could be expired, we need to temporarily disable timestamp checks
        Node._toggle_enable_partial_match_expiration(False)
        for partial_match in matches_to_flush:
            super()._add_partial_match(partial_match)
        Node._toggle_enable_partial_match_expiration(True)

    def get_event_definitions(self):
        """
        This is an ugly temporary hack to support multiple chained negation operators. As this prevents different
        negative events from having mutual conditions, this implementation is highly undesirable and will be removed
        in future.
        """
        return self._positive_subtree.get_event_definitions()

    def _try_create_new_matches(self, new_partial_match: PartialMatch, partial_matches_to_compare: List[PartialMatch],
                                first_event_defs: List[Tuple[int, QItem]], second_event_defs: List[Tuple[int, QItem]]):
        """
        The flow of this method is the opposite of the one its superclass implements. For each pair of a positive and a
        negative partial match, we combine the two sides to form a new partial match, validate it, and then do nothing
        if the validation succeeds (i.e., the negative part invalidated the positive one), and transfer the positive
        match up the tree if the validation fails.
        """
        positive_events = new_partial_match.events
        for partial_match in partial_matches_to_compare:
            negative_events = partial_match.events
            combined_event_list = self._merge_events_for_new_match(first_event_defs, second_event_defs,
                                                                   positive_events, negative_events)
            if self._validate_new_match(combined_event_list):
                # this match should not be transferred
                # TODO: the rejected positive partial match should be explicitly removed to save space
                return
        # no negative match invalidated the positive one - we can go on
        self._propagate_partial_match(positive_events)

    def _add_partial_match(self, pm: PartialMatch):
        """
        If this node can receive unbounded negative events and is the deepest node in the tree to do so, a
        successfully evaluated partial match must be added to a dedicated waiting list rather than propagated normally.
        """
        if self.__is_first_unbounded_negative_node():
            self.__pending_partial_matches.append(pm)
        else:
            super()._add_partial_match(pm)

    def handle_new_partial_match(self, partial_match_source: Node):
        """
        For positive partial matches, activates the flow of the superclass. For negative partial matches, does nothing
        for bounded events (as nothing should be done in this case), otherwise checks whether existing positive matches
        must be invalidated and handles them accordingly.
        """
        if partial_match_source == self._positive_subtree:
            # a new positive partial match has arrived
            super().handle_new_partial_match(partial_match_source)
            return
        # a new negative partial match has arrived
        if not self.__is_unbounded:
            # no unbounded negatives - there is nothing to do
            return

        # this partial match contains unbounded negative events
        first_unbounded_node = self.get_first_unbounded_negative_node()
        positive_event_defs = first_unbounded_node.get_event_definitions()

        unbounded_negative_partial_match = partial_match_source.get_last_unhandled_partial_match()
        negative_event_defs = partial_match_source.get_event_definitions()

        matches_to_keep = []
        for positive_partial_match in first_unbounded_node.__pending_partial_matches:
            combined_event_list = self._merge_events_for_new_match(positive_event_defs,
                                                                   negative_event_defs,
                                                                   positive_partial_match.events,
                                                                   unbounded_negative_partial_match.events)
            if not self._validate_new_match(combined_event_list):
                # this positive match should still be kept
                matches_to_keep.append(positive_partial_match)

        first_unbounded_node.__pending_partial_matches = matches_to_keep

    def get_first_unbounded_negative_node(self):
        """
        Returns the deepest unbounded node in the tree. This node keeps the partial matches that are pending release
        due to the presence of unbounded negative events in the pattern.
        """
        if not self.__is_unbounded:
            return None
        return self if self.__is_first_unbounded_negative_node() \
            else self._positive_subtree.get_first_unbounded_negative_node()

    def __is_first_unbounded_negative_node(self):
        """
        Returns True if this node is the first unbounded negative node and False otherwise.
        """
        return self.__is_unbounded and \
               (not isinstance(self._positive_subtree, NegationNode) or not self._positive_subtree.__is_unbounded)

    def create_storage_unit(self, storage_params: TreeStorageParameters, sorting_key: callable = None,
                            rel_op: RelopTypes = None, equation_side: EquationSides = None,
                            sort_by_first_timestamp: bool = False):
        """
        For now, only the most trivial storage settings will be supported by negative nodes.
        """
        self._init_storage_unit(storage_params, sorting_key, rel_op, equation_side)
        self._left_subtree.create_storage_unit(storage_params)
        self._right_subtree.create_storage_unit(storage_params)


class NegativeAndNode(NegationNode):
    """
    An internal node representing a negative conjunction operator.
    """
    def __init__(self, sliding_window: timedelta, is_unbounded: bool, parent: Node = None,
                 event_defs: List[Tuple[int, QItem]] = None,
                 left: Node = None, right: Node = None):
        super().__init__(sliding_window, is_unbounded, AndOperator, parent, event_defs, left, right)

    def get_structure_summary(self):
        return ("NAnd",
                self._left_subtree.get_structure_summary(),
                self._right_subtree.get_structure_summary())


class NegativeSeqNode(NegationNode):
    """
    An internal node representing a negative sequence operator.
    Unfortunately, this class contains some code duplication from SeqNode to avoid diamond inheritance.
    """
    def __init__(self, sliding_window: timedelta, is_unbounded: bool, parent: Node = None,
                 event_defs: List[Tuple[int, QItem]] = None,
                 left: Node = None, right: Node = None):
        super().__init__(sliding_window, is_unbounded, SeqOperator, parent, event_defs, left, right)

    def get_structure_summary(self):
        return ("NSeq",
                self._left_subtree.get_structure_summary(),
                self._right_subtree.get_structure_summary())

    def _set_event_definitions(self,
                               left_event_defs: List[Tuple[int, QItem]], right_event_defs: List[Tuple[int, QItem]]):
        self._event_defs = merge(left_event_defs, right_event_defs, key=lambda x: x[0])

    def _validate_new_match(self, events_for_new_match: List[Event]):
        if not is_sorted(events_for_new_match, key=lambda x: x.timestamp):
            return False
        return super()._validate_new_match(events_for_new_match)

    def _merge_events_for_new_match(self,
                                    first_event_defs: List[Tuple[int, QItem]],
                                    second_event_defs: List[Tuple[int, QItem]],
                                    first_event_list: List[Event],
                                    second_event_list: List[Event]):
        return merge_according_to(first_event_defs, second_event_defs,
                                  first_event_list, second_event_list, key=lambda x: x[0])


class KleeneClosureNode(UnaryNode):
    """
    An internal node representing a Kleene closure operator.
    It generates and propagates sets of partial matches provided by its sole child.
    """
    def __init__(self, sliding_window: timedelta, min_size, max_size, parent: Node = None):
        super().__init__(sliding_window, parent)
        self.__min_size = min_size
        self.__max_size = max_size

    def handle_new_partial_match(self, partial_match_source: Node):
        """
        Reacts upon a notification of a new partial match available at the child by generating, validating, and
        propagating all sets of partial matches containing this new partial match.
        Note: this method strictly assumes that the last partial match in the child storage is the one to cause the
        method call (could not function properly in a parallelized implementation of the evaluation tree).
        """
        if self._child is None:
            raise Exception()  # should never happen

        new_partial_match = self._child.get_last_unhandled_partial_match()
        self._child.clean_expired_partial_matches(new_partial_match.last_timestamp)

        # create partial match sets containing the new partial match that triggered this method
        child_matches_powerset = self.__create_child_matches_powerset()

        for partial_match_set in child_matches_powerset:
            # create and propagate the new match
            # TODO: except for the time window constraint, no validation is supported as of now
            events_for_partial_match = KleeneClosureNode.partial_match_set_to_event_list(partial_match_set)
            self._validate_and_propagate_partial_match(events_for_partial_match)

    def _validate_new_match(self, events_for_new_match: List[Event]):
        """
        Validates the condition stored in this node on the given set of events.
        """
        return self._condition.eval([e.payload for e in events_for_new_match])

    def __create_child_matches_powerset(self):
        """
        This method is a generator returning all subsets of currently available partial matches of this node child.
        As this method is always invoked following a notification regarding a new partial match received from the child,
        only the subsets containing this new partial match (which is assumed to be the last partial match in the child
        list) are generated.
        The subsets are enforced to satisfy the minimal and maximal size constraints.
        The maximal size constraint is enforced recursively to save as many computations as possible.
        The minimal size constraint on the other hand is enforced via post-processing filtering due to negligible
        overhead.
        """
        child_partial_matches = self._child.get_partial_matches()
        if len(child_partial_matches) == 0:
            return []
        last_partial_match = child_partial_matches[-1]
        # create subsets for all but the last element
        actual_max_size = self.__max_size if self.__max_size is not None else len(child_partial_matches)
        generated_powerset = recursive_powerset_generator(child_partial_matches[:-1], actual_max_size - 1)
        # add the last item to all previously created subsets
        result_powerset = [item + [last_partial_match] for item in generated_powerset]
        # enforce minimal size limit
        result_powerset = [item for item in result_powerset if self.__min_size <= len(item)]
        return result_powerset

    def __assign_kc_formula(self, formula: Formula):
        names = {item[1].name for item in self._event_defs}
        condition = formula.get_formula_of(names, True)
        self._condition = condition if condition else TrueFormula()

    def __consume_kc_formula(self, formula: Formula):
        names = {item[1].name for item in self._event_defs}
        formula.consume_formula_of(names, True)

    # assign TrueFormula for future update if necessary
    def _assign_formula(self, formula: Formula):
        self._condition = TrueFormula()

    def _consume_formula(self, formula: Formula):
        return

    def apply_kc_formulas(self, kc_conditions):
        self.__assign_kc_formula(kc_conditions)
        self.__consume_kc_formula(kc_conditions)

    def get_structure_summary(self):
        return "KC", self._child.get_structure_summary()

    @staticmethod
    def partial_match_set_to_event_list(partial_match_set: List[PartialMatch]):
        """
        Converts a set of partial matches into a single list containing all primitive events of the partial
        matches in the set.
        TODO: this is not the way this operator should work!
        """
        min_timestamp = None
        max_timestamp = None
        events = []
        for match in partial_match_set:
            min_timestamp = match.first_timestamp if min_timestamp is None else min(min_timestamp, match.first_timestamp)
            max_timestamp = match.last_timestamp if max_timestamp is None else max(max_timestamp, match.last_timestamp)
            events.extend(match.events)
        return events


class Tree:
    """
    Represents an evaluation tree. Implements the functionality of constructing an actual tree from a "tree positive_structure"
    object returned by a tree builder. Other than that, merely acts as a proxy to the tree root node.
    """
    def __init__(self, tree_structure: tuple, pattern: Pattern, storage_params: TreeStorageParameters):
        self.__root = self.__construct_tree(pattern.positive_structure, tree_structure,
                                            Tree.__get_operator_arg_list(pattern.positive_structure),
                                            pattern.window, None, pattern.consumption_policy)

        if pattern.consumption_policy is not None and \
                pattern.consumption_policy.should_register_event_type_as_single(True):
            for event_type in pattern.consumption_policy.single_types:
                self.__root.register_single_event_type(event_type)

        if pattern.negative_structure is not None:
            self.__adjust_leaf_indices(pattern)
            self.__add_negative_tree_structure(pattern)

        kc_nodes = self.__root.apply_formula(pattern.condition)
        remaining_conditions = self.__apply_kc_formulas(kc_nodes, pattern.condition)
        if remaining_conditions.get_num_formulas() > 0:
            print('Warning!!!\nUnused formulas found after applying formula has finished!')
        self.__root.create_storage_unit(storage_params)

    @staticmethod
    def __apply_kc_formulas(kc_nodes, kc_conditions):
        for kc_node in kc_nodes:
            kc_node.apply_kc_formulas(kc_conditions)
        return kc_conditions

    def __adjust_leaf_indices(self, pattern: Pattern):
        """
        Fixes the values of the leaf indices in the positive tree to take the negative events into account.
        """
        leaf_mapping = {}
        for leaf in self.get_leaves():
            current_index = leaf.get_leaf_index()
            correct_index = pattern.get_index_by_event_name(leaf.get_event_name())
            leaf_mapping[current_index] = correct_index
        self.__update_event_defs(self.__root, leaf_mapping)

    def __update_event_defs(self, node: Node, leaf_mapping: Dict[int, int]):
        """
        Recursively modifies the event indices in the tree specified by the given node.
        """
        if isinstance(node, LeafNode):
            node.set_leaf_index(leaf_mapping[node.get_leaf_index()])
            return
        # this node is an internal node
        event_defs = node.get_event_definitions()
        # no list comprehension is used since we modify the original list
        for i in range(len(event_defs)):
            event_def = event_defs[i]
            event_defs[i] = (leaf_mapping[event_def[0]], event_def[1])
        self.__update_event_defs(node.get_left_subtree(), leaf_mapping)
        self.__update_event_defs(node.get_right_subtree(), leaf_mapping)

    def __add_negative_tree_structure(self, pattern: Pattern):
        """
        Adds the negative nodes at the root of the tree.
        """
        top_operator = pattern.full_structure.get_top_operator()
        negative_event_list = pattern.negative_structure.get_args()
        current_root = self.__root
        for negation_operator in negative_event_list:
            if top_operator == SeqOperator:
                new_root = NegativeSeqNode(pattern.window,
                                           is_unbounded=Tree.__is_unbounded_negative_event(pattern, negation_operator))
            elif top_operator == AndOperator:
                new_root = NegativeAndNode(pattern.window,
                                           is_unbounded=Tree.__is_unbounded_negative_event(pattern, negation_operator))
            else:
                raise Exception("Unsupported operator for negation: %s" % (top_operator,))
            negative_event = negation_operator.arg
            leaf_index = pattern.get_index_by_event_name(negative_event.name)
            negative_leaf = LeafNode(pattern.window, leaf_index, negative_event, new_root)
            new_root.set_subtrees(current_root, negative_leaf)
            negative_leaf.set_parent(new_root)
            current_root.set_parent(new_root)
            current_root = new_root
        self.__root = current_root

    def get_leaves(self):
        return self.__root.get_leaves()

    def get_matches(self):
        while self.__root.has_partial_matches():
            yield self.__root.consume_first_partial_match().events

    def get_structure_summary(self):
        """
        Returns a tuple summarizing the structure of the tree.
        """
        return self.__root.get_structure_summary()

    @staticmethod
    def __get_operator_arg_list(operator: PatternStructure):
        """
        Returns the list of arguments of the given operator for the tree construction process.
        """
        if isinstance(operator, CompositeStructure):
            return operator.args
        if isinstance(operator, UnaryStructure):
            return [operator.arg]
        # a QItem
        return [operator]

    @staticmethod
    def __create_internal_node_by_operator(operator: PatternStructure, sliding_window: timedelta, parent: Node = None):
        """
        Creates an internal node representing a given operator.
        Note that negation node types are intentionally not supported here since the negative part of a pattern is
        added in a separate construction stage.
        """
        operator_type = operator.get_top_operator()
        if operator_type == SeqOperator:
            return SeqNode(sliding_window, parent)
        if operator_type == AndOperator:
            return AndNode(sliding_window, parent)
        if operator_type == KleeneClosureOperator:
            return KleeneClosureNode(sliding_window, operator.min_size, operator.max_size, parent)
        raise Exception("Unknown or unsupported operator %s" % (operator_type,))

    def __handle_primitive_event_or_nested_structure(self, event_index: int, current_operator: PatternStructure,
                                                     sliding_window: timedelta, parent: Node,
                                                     consumption_policy: ConsumptionPolicy):
        """
        Constructs a single leaf node or a subtree with nested structure according to the input parameters.
        """
        if isinstance(current_operator, QItem):
            # the current operator is a primitive event - we should simply create a leaf
            event = current_operator
            if consumption_policy is not None and \
                    consumption_policy.should_register_event_type_as_single(False, event.type):
                parent.register_single_event_type(event.type)
            return LeafNode(sliding_window, event_index, current_operator, parent)

        if isinstance(current_operator, UnaryStructure):
            # the current operator is a unary operator hiding a nested pattern structure
            unary_node = self.__create_internal_node_by_operator(current_operator, sliding_window, parent)
            nested_operator = current_operator.arg
            child = self.__construct_tree(nested_operator, Tree.__create_nested_structure(nested_operator),
                                          Tree.__get_operator_arg_list(nested_operator), sliding_window, unary_node,
                                          consumption_policy)
            unary_node.set_subtree(child)
            return unary_node

        # the current operator is a nested binary operator
        return self.__construct_tree(current_operator, Tree.__create_nested_structure(current_operator),
                                     current_operator.args, sliding_window, parent, consumption_policy)

    def __construct_tree(self, root_operator: PatternStructure, tree_structure: tuple or int,
                         args: List[PatternStructure], sliding_window: timedelta, parent: Node,
                         consumption_policy: ConsumptionPolicy):
        """
        Recursively builds an evaluation tree according to the specified structure.
        """
        if isinstance(root_operator, UnaryStructure) and parent is None:
            # a special case where the top operator of the entire pattern is an unary operator
            return self.__handle_primitive_event_or_nested_structure(tree_structure, root_operator,
                                                                     sliding_window, parent, consumption_policy)

        if type(tree_structure) == int:
            # either a leaf node or an unary operator encapsulating a nested structure
            return self.__handle_primitive_event_or_nested_structure(tree_structure, args[tree_structure],
                                                                     sliding_window, parent, consumption_policy)

        # an internal node
        current = self.__create_internal_node_by_operator(root_operator, sliding_window, parent)
        left_structure, right_structure = tree_structure
        left = self.__construct_tree(root_operator, left_structure, args, sliding_window, current, consumption_policy)
        right = self.__construct_tree(root_operator, right_structure, args, sliding_window, current, consumption_policy)
        current.set_subtrees(left, right)
        return current

    def get_last_matches(self):
        """
        After the system run is completed, retrieves and returns the last pending matches.
        As of now, the only case in which such matches may exist is if a pattern contains an unbounded negative event
        (e.g., SEQ(A,B,NOT(C)), in which case positive partial matches wait for timeout before proceeding to the root.
        """
        if not isinstance(self.__root, NegationNode):
            return []
        # this is the node that contains the pending matches
        first_unbounded_negative_node = self.__root.get_first_unbounded_negative_node()
        if first_unbounded_negative_node is None:
            return []
        first_unbounded_negative_node.flush_pending_matches()
        # the pending matches were released and have hopefully reached the root
        return self.get_matches()

    @staticmethod
    def __create_nested_structure(nested_operator):
        """
        This method is a temporal hack, hopefully it will be removed soon.
        # TODO: calculate the evaluation order in the way it should work - using a tree plan builder
        """
        order = list(range(len(nested_operator.args))) if isinstance(nested_operator, CompositeStructure) else [0]
        ret = order[0]
        for i in range(1, len(order)):
            ret = (ret, order[i])
        return ret

    @staticmethod
    def __is_unbounded_negative_event(pattern: Pattern, negation_operator: NegationOperator):
        """
        Returns True if the negative event represented by the given operator is unbounded (i.e., can appear after the
        entire match is ready and invalidate it) and False otherwise.
        """
        if pattern.full_structure.get_top_operator() != SeqOperator:
            return True
        # for a sequence pattern, a negative event is unbounded if no positive events follow it
        # the implementation below assumes a flat sequence
        sequence_elements = pattern.full_structure.get_args()
        operator_index = sequence_elements.index(negation_operator)
        for i in range(operator_index + 1, len(sequence_elements)):
            if isinstance(sequence_elements[i], QItem):
                return False
        return True


class TreeBasedEvaluationMechanism(EvaluationMechanism):
    """
    An implementation of the tree-based evaluation mechanism.
    """
    def __init__(self, pattern: Pattern, tree_structure: tuple, storage_params: TreeStorageParameters):
        self.__tree = Tree(tree_structure, pattern, storage_params)
        self.__pattern = pattern
        self.__freeze_map = {}
        self.__active_freezers = []
        self.__event_types_listeners = {}

        if pattern.consumption_policy is not None and pattern.consumption_policy.freeze_names is not None:
            self.__init_freeze_map()

    def eval(self, events: Stream, matches: Stream, is_async=False, file_path=None, time_limit: int = None):
        """
        Activates the tree evaluation mechanism on the input event stream and reports all found patter matches to the
        given output stream.
        """
        self.__register_event_listeners()
        start_time = time.time()
        for event in events:
            if time_limit is not None:
                if time.time() - start_time > time_limit:
                    matches.close()
                    return
            if event.type not in self.__event_types_listeners.keys():
                continue
            self.__remove_expired_freezers(event)
            for leaf in self.__event_types_listeners[event.type]:
                if self.__should_ignore_events_on_leaf(leaf):
                    continue
                self.__try_register_freezer(event, leaf)
                leaf.handle_event(event)
            for match in self.__tree.get_matches():
                matches.add_item(PatternMatch(match))
                self.__remove_matched_freezers(match)
                if is_async:
                        f = open(file_path, "a", encoding='utf-8')
                        for itr in match:
                            f.write("%s \n" % str(itr.payload))
                        f.write("\n")
                        f.close()

        # Now that we finished the input stream, if there were some pending matches somewhere in the tree, we will
        # collect them now
        for match in self.__tree.get_last_matches():
            matches.add_item(PatternMatch(match))
        matches.close()

    def __register_event_listeners(self):
        """
        Register leaf listeners for event types.
        """
        self.__event_types_listeners = {}
        for leaf in self.__tree.get_leaves():
            event_type = leaf.get_event_type()
            if event_type in self.__event_types_listeners.keys():
                self.__event_types_listeners[event_type].append(leaf)
            else:
                self.__event_types_listeners[event_type] = [leaf]

    def __init_freeze_map(self):
        """
        For each event type specified by the user to be a 'freezer', that is, an event type whose appearance blocks
        initialization of new sequences until it is either matched or expires, this method calculates the list of
        leaves to be disabled.
        """
        sequences = self.__pattern.extract_flat_sequences()
        for freezer_event_name in self.__pattern.consumption_policy.freeze_names:
            current_event_name_set = set()
            for sequence in sequences:
                if freezer_event_name not in sequence:
                    continue
                for name in sequence:
                    current_event_name_set.add(name)
                    if name == freezer_event_name:
                        break
            if len(current_event_name_set) > 0:
                self.__freeze_map[freezer_event_name] = current_event_name_set

    def __should_ignore_events_on_leaf(self, leaf: LeafNode):
        """
        If the 'freeze' consumption policy is enabled, checks whether the given event should be dropped based on it.
        """
        if len(self.__freeze_map) == 0:
            # freeze option disabled
            return False
        for freezer in self.__active_freezers:
            for freezer_leaf in self.__event_types_listeners[freezer.type]:
                if freezer_leaf.get_event_name() not in self.__freeze_map:
                    continue
                if leaf.get_event_name() in self.__freeze_map[freezer_leaf.get_event_name()]:
                    return True
        return False

    def __try_register_freezer(self, event: Event, leaf: LeafNode):
        """
        Check whether the current event is a freezer event, and, if positive, register it.
        """
        if leaf.get_event_name() in self.__freeze_map.keys():
            self.__active_freezers.append(event)

    def __remove_matched_freezers(self, match: List[Event]):
        """
        Removes the freezers that have been matched.
        """
        if len(self.__freeze_map) == 0:
            # freeze option disabled
            return False
        self.__active_freezers = [freezer for freezer in self.__active_freezers if freezer not in match]

    def __remove_expired_freezers(self, event: Event):
        """
        Removes the freezers that have been expired.
        """
        if len(self.__freeze_map) == 0:
            # freeze option disabled
            return False
        self.__active_freezers = [freezer for freezer in self.__active_freezers
                                  if event.timestamp - freezer.timestamp <= self.__pattern.window]

    def get_structure_summary(self):
        return self.__tree.get_structure_summary()
