from abc import ABC
from datetime import timedelta, datetime
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, QItem, OrOperator, NegationOperator
from base.Formula import TrueFormula, Formula
from evaluation.PartialMatch import PartialMatch
from misc.IOUtils import Stream
from typing import List, Tuple, Dict
from base.Event import Event
from misc.Utils import merge, merge_according_to, is_sorted, find_partial_match_by_timestamp
from base.PatternMatch import PatternMatch
from evaluation.EvaluationMechanism import EvaluationMechanism
from queue import Queue


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
        self._partial_matches = []
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
        count = find_partial_match_by_timestamp(self._partial_matches, last_timestamp - self._sliding_window)
        self._partial_matches = self._partial_matches[count:]
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
        As of now, the insertion is always by the timestamp, and the partial matches are stored in a list sorted by
        timestamp. Therefore, the insertion operation is performed in O(log n).
        """
        index = find_partial_match_by_timestamp(self._partial_matches, pm.first_timestamp)
        self._partial_matches.insert(index, pm)
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
            if event.event_type not in self._single_event_types:
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

    def get_partial_matches(self):
        """
        Returns the currently stored partial matches.
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
        raise NotImplementedError()

    def get_event_definitions(self):
        """
        Returns the specifications of all events collected by this tree - to be implemented by subclasses.
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

    def apply_formula(self, formula: Formula):
        condition = formula.get_formula_of(self.__event_name)
        if condition is not None:
            self._condition = condition

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


class InternalNode(Node):
    """
    An internal node connects two subtrees, i.e., two subpatterns of the evaluated pattern.
    """
    def __init__(self, sliding_window: timedelta, parent: Node = None, event_defs: List[Tuple[int, QItem]] = None,
                 left: Node = None, right: Node = None):
        super().__init__(sliding_window, parent)
        self._event_defs = event_defs
        self._left_subtree = left
        self._right_subtree = right

    def get_leaves(self):
        result = []
        if self._left_subtree is not None:
            result += self._left_subtree.get_leaves()
        if self._right_subtree is not None:
            result += self._right_subtree.get_leaves()
        return result

    def apply_formula(self, formula: Formula):
        names = {item[1].name for item in self._event_defs}
        condition = formula.get_formula_of(names)
        self._condition = condition if condition else TrueFormula()
        self._left_subtree.apply_formula(formula)
        self._right_subtree.apply_formula(formula)

    def get_event_definitions(self):
        return self._event_defs

    def _set_event_definitions(self,
                               left_event_defs: List[Tuple[int, QItem]], right_event_defs: List[Tuple[int, QItem]]):
        """
        A helper function for collecting the event definitions from subtrees. To be overridden by subclasses.
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
        first_event_defs = partial_match_source.get_event_definitions()
        other_subtree.clean_expired_partial_matches(new_partial_match.last_timestamp)
        partial_matches_to_compare = other_subtree.get_partial_matches()
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


class AndNode(InternalNode):
    """
    An internal node representing an "AND" operator.
    """
    pass


class SeqNode(InternalNode):
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


class NegationNode(InternalNode):
    """
    An internal node representing a negation operator.
    """
    def __init__(self, sliding_window: timedelta, is_last: bool, top_operator, parent: Node = None,
                 event_defs: List[Tuple[int, QItem]] = None,
                 left: Node = None, right: Node = None):
        super().__init__(sliding_window, parent, event_defs, left, right)

        """
        Negation operators that have no "positive" operators after them in the pattern have the flag is_last on
        """
        self.is_last = is_last
        self.top_operator = top_operator

        """
        Contains PMs that match the pattern, but may be invalidated by a negative event later (when the pattern ends
        with a not operator)
        We wait for them to exceed the time window and therefore can't be invalidated anymore
        """
        self.waiting_for_time_out = []

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
            self.waiting_for_time_out = sorted(self.waiting_for_time_out, key=lambda x: x.first_timestamp)
            count = find_partial_match_by_timestamp(self.waiting_for_time_out, last_timestamp - self._sliding_window)
            matches_to_flush = self.waiting_for_time_out[:count]
            self.waiting_for_time_out = self.waiting_for_time_out[count:]
        else:
            matches_to_flush = self.waiting_for_time_out
        # since matches_to_flush could be expired, we need to temporarily disable timestamp checks
        Node._toggle_enable_partial_match_expiration(False)
        for partial_match in matches_to_flush:
            super()._add_partial_match(partial_match)
        Node._toggle_enable_partial_match_expiration(True)

    def _set_event_definitions(self,
                               left_event_defs: List[Tuple[int, QItem]], right_event_defs: List[Tuple[int, QItem]]):
        self._event_defs = merge(left_event_defs, right_event_defs, key=lambda x: x[0])

    # In an NegationNode, the event_def represents all the positives events plus the negative event we are currently checking
    def get_event_definitions(self):  # to support multiple neg
        return self._left_subtree.get_event_definitions()

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

    def _validate_new_match(self, events_for_new_match: List[Event]):
        if self.top_operator == SeqOperator and not is_sorted(events_for_new_match, key=lambda x: x.timestamp):
            return False
        return super()._validate_new_match(events_for_new_match)

    def _merge_events_for_new_match(self,
                                    first_event_defs: List[Tuple[int, QItem]],
                                    second_event_defs: List[Tuple[int, QItem]],
                                    first_event_list: List[Event],
                                    second_event_list: List[Event]):
        if self.top_operator == SeqOperator:
            return merge_according_to(first_event_defs, second_event_defs,
                                      first_event_list, second_event_list, key=lambda x: x[0])
        return super()._merge_events_for_new_match(first_event_defs, second_event_defs,
                                                   first_event_list, second_event_list)

    def _add_partial_match(self, pm: PartialMatch):
        """
        If this node can receive unbounded negative events and is the deepest node in the tree to do so, a
        successfully evaluated partial match must be added to a dedicated waiting list rather than propagated normally.
        """
        if self.__is_first_unbounded_negative_node():
            self.waiting_for_time_out.append(pm)
        else:
            super()._add_partial_match(pm)

    def handle_new_partial_match(self, partial_match_source: Node):
        """
        For positive partial matches, activates the flow of the superclass. For negative partial matches, does nothing
        for bounded events (as nothing should be done in this case), otherwise checks whether existing positive matches
        must be invalidated and handles them accordingly.
        """
        if partial_match_source == self._left_subtree:
            # a new positive partial match has arrived
            super().handle_new_partial_match(partial_match_source)
            return
        # a new negative partial match has arrived
        if not self.is_last:
            # no unbounded negatives - there is nothing to do
            return

        # this partial match contains unbounded negative events
        first_unbounded_node = self.get_first_unbounded_negative_node()
        positive_event_defs = first_unbounded_node.get_event_definitions()

        unbounded_negative_partial_match = partial_match_source.get_last_unhandled_partial_match()
        negative_event_defs = partial_match_source.get_event_definitions()

        matches_to_keep = []
        for positive_partial_match in first_unbounded_node.waiting_for_time_out:
            combined_event_list = self._merge_events_for_new_match(positive_event_defs,
                                                                   negative_event_defs,
                                                                   positive_partial_match.events,
                                                                   unbounded_negative_partial_match.events)
            if not self._validate_new_match(combined_event_list):
                # this positive match should still be kept
                matches_to_keep.append(positive_partial_match)

        first_unbounded_node.waiting_for_time_out = matches_to_keep

    def get_first_unbounded_negative_node(self):
        """
        Returns the deepest unbounded node in the tree. This node keeps the partial matches that are pending release
        due to the presence of unbounded negative events in the pattern.
        """
        if not self.is_last:
            return None
        return self if self.__is_first_unbounded_negative_node() \
            else self._left_subtree.get_first_unbounded_negative_node()

    def __is_first_unbounded_negative_node(self):
        """
        Returns True if this node is the first unbounded negative node and False otherwise.
        """
        return self.is_last and (type(self._left_subtree) != NegationNode or not self._left_subtree.is_last)


class Tree:
    """
    Represents an evaluation tree. Implements the functionality of constructing an actual tree from a "tree positive_structure"
    object returned by a tree builder. Other than that, merely acts as a proxy to the tree root node.
    """
    def __init__(self, tree_structure: tuple, pattern: Pattern):
        # Note that right now only "flat" sequence patterns and "flat" conjunction patterns are supported
        self.__root = Tree.__construct_tree(pattern.positive_structure.get_top_operator() == SeqOperator,
                                            tree_structure, pattern.positive_structure.args, pattern.window)

        if pattern.negative_structure is not None:
            self.__adjust_leaf_indices(pattern)
            self.__add_negative_tree_structure(pattern)

        self.__root.apply_formula(pattern.condition)

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
            new_root = NegationNode(pattern.window,
                                    is_last=Tree.__is_unbounded_negative_event(pattern, negation_operator),
                                    top_operator=top_operator)
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
    def __construct_tree(is_sequence: bool, tree_structure: tuple or int, args: List[QItem],
                         sliding_window: timedelta, parent: Node = None):
        """
        Constructs the actual evaluation tree given a tree structure.
        """
        if type(tree_structure) == int:
            return LeafNode(sliding_window, tree_structure, args[tree_structure], parent)
        current = SeqNode(sliding_window, parent) if is_sequence else AndNode(sliding_window, parent)
        left_structure, right_structure = tree_structure
        left = Tree.__construct_tree(is_sequence, left_structure, args, sliding_window, current)
        right = Tree.__construct_tree(is_sequence, right_structure, args, sliding_window, current)

        current.set_subtrees(left, right)
        return current

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
    def __init__(self, pattern: Pattern, tree_structure: tuple):
        self.__tree = Tree(tree_structure, pattern)

    def eval(self, events: Stream, matches: Stream):
        event_types_listeners = {}
        # register leaf listeners for event types.
        for leaf in self.__tree.get_leaves():
            event_type = leaf.get_event_type()
            if event_type in event_types_listeners.keys():
                event_types_listeners[event_type].append(leaf)
            else:
                event_types_listeners[event_type] = [leaf]

        # Send events to listening leaves.
        for event in events:
            if event.type in event_types_listeners.keys():
                for leaf in event_types_listeners[event.type]:
                    leaf.handle_event(event)
                    for match in self.__tree.get_matches():
                        matches.add_item(PatternMatch(match))

        # Now that we finished the input stream, if there were some pending matches somewhere in the tree, we will
        # collect them now
        for match in self.__tree.get_last_matches():
            matches.add_item(PatternMatch(match))

        matches.close()
