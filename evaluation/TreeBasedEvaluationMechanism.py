from abc import ABC
from datetime import timedelta, datetime
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, QItem, OrOperator
from base.Formula import TrueFormula, Formula
from evaluation.PartialMatch import PartialMatch
from misc.IOUtils import Stream
from typing import List, Tuple
from base.Event import Event
from misc.Utils import merge, merge_according_to, is_sorted, find_partial_match_by_timestamp
from base.PatternMatch import PatternMatch
from evaluation.EvaluationMechanism import EvaluationMechanism
from queue import Queue


class Node(ABC):
    """
    This class represents a single node of an evaluation tree.
    """
    def __init__(self, sliding_window: timedelta, parent):
        self._parent = parent
        self._sliding_window = sliding_window
        self._partial_matches = []
        self._condition = TrueFormula()
        # matches that were not yet pushed to the parent for further processing
        self._unhandled_partial_matches = Queue()

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
        Get root of tree
        """
        node = self
        while node._parent is not None:
            node = node._parent
        return node

    def clean_expired_partial_matches(self, last_timestamp: datetime):
        """
        Removes partial matches whose earliest timestamp violates the time window constraint.
        """
        if self._sliding_window == timedelta.max:
            return
        count = find_partial_match_by_timestamp(self._partial_matches, last_timestamp - self._sliding_window)
        self._partial_matches = self._partial_matches[count:]

    def add_partial_match(self, pm: PartialMatch):
        """
        Registers a new partial match at this node.
        As of now, the insertion is always by the timestamp, and the partial matches are stored in a list sorted by
        timestamp. Therefore, the insertion operation is performed in O(log n).
        """
        index = find_partial_match_by_timestamp(self._partial_matches, pm.first_timestamp)
        self._partial_matches.insert(index, pm)
        if self._parent is not None:
            self._unhandled_partial_matches.put(pm)

    def get_partial_matches(self):
        """
        Returns the currently stored partial matches.
        """
        return self._partial_matches

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

    def fix_eventdef(self, index: int, event_name: str):
        raise NotImplementedError()

class LeafNode(Node):
    """
    A leaf node is responsible for a single event type of the pattern.
    """
    def __init__(self, sliding_window: timedelta, leaf_index: int, leaf_qitem: QItem, parent: Node):
        super().__init__(sliding_window, parent)
        self.__leaf_index = leaf_index
        self.__event_name = leaf_qitem.name
        self.__event_type = leaf_qitem.event_type

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
        return self.__leaf_index

    def set_leaf_index(self, index: int):
        """
        Change the leaf_index of the leaf
        """
        self.__leaf_index = index

    def handle_event(self, event: Event):
        """
        Inserts the given event to this leaf.
        """
        self.clean_expired_partial_matches(event.timestamp)

        # get event's qitem and make a binding to evaluate formula for the new event.
        binding = {self.__event_name: event.payload}

        if not self._condition.eval(binding):
            return

        self.add_partial_match(PartialMatch([event]))
        if self._parent is not None:
            self._parent.handle_new_partial_match(self)


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
        self._left_subtree.apply_formula(self._condition)
        self._right_subtree.apply_formula(self._condition)

    def get_event_definitions(self):
        return self._event_defs

    def _set_event_definitions(self,
                               left_event_defs: List[Tuple[int, QItem]], right_event_defs: List[Tuple[int, QItem]]):
        """
        A helper function for collecting the event definitions from subtrees. To be overridden by subclasses.
        """
        self._event_defs = left_event_defs + right_event_defs

    def set_subtrees(self, left: Node, right: Node):
        """
        Sets the subtrees of this node.
        """
        self._left_subtree = left
        self._right_subtree = right
        self._set_event_definitions(self._left_subtree.get_event_definitions(),
                                    self._right_subtree.get_event_definitions())

    def fix_eventdef(self, index: int, event_name: str):
        """
        Change the index of the corresponding event_def (according to event_name) to the value that we received in param
        """
        founded = False
        for i in range(len(self._event_defs)):
            item = self._event_defs[i]
            if event_name == item[1].name:
                new_tuple = (index, item[1])
                self._event_defs[i] = new_tuple
                founded = True
                break
        if founded is False:
            raise Exception("event_name not found in event def")
        if self._parent is not None:
            self._parent.fix_eventdef(index, event_name)

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

        self.clean_expired_partial_matches(new_partial_match.last_timestamp)

        # given a partial match from one subtree, for each partial match
        # in the other subtree we check for new partial matches in this node.
        for partialMatch in partial_matches_to_compare:
            self._try_create_new_match(new_partial_match, partialMatch, first_event_defs, second_event_defs)

    def _try_create_new_match(self,
                              first_partial_match: PartialMatch, second_partial_match: PartialMatch,
                              first_event_defs: List[Tuple[int, QItem]], second_event_defs: List[Tuple[int, QItem]]):
        """
        Verifies all the conditions for creating a new partial match and creates it if all constraints are satisfied.
        """
        if self._sliding_window != timedelta.max and \
                abs(first_partial_match.last_timestamp - second_partial_match.first_timestamp) > self._sliding_window:
            return
        events_for_new_match = self._merge_events_for_new_match(first_event_defs, second_event_defs,
                                                                first_partial_match.events, second_partial_match.events)

        if not self._validate_new_match(events_for_new_match):
            return
        self.add_partial_match(PartialMatch(events_for_new_match))
        if self._parent is not None:
            self._parent.handle_new_partial_match(self)

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
    An internal node representing a  NegationOperator.
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

        """
        Contains PMs that match the whole pattern and were in waiting_for_timeout, and now can't be invalidated anymore
        When we finish all the stream of events we handle them and put them in the output
        Is used only in root
        We need this field (instead of putting the matches directly in root.partial_match) because these are 
        expired match : they will be removed from root.partial_match when we call clean_expired
        """
        self.matches_to_handle_at_EOF = []

    def clean_expired_partial_matches(self, last_timestamp: datetime):
        super().clean_expired_partial_matches(last_timestamp)

        """
        "waiting for timeout" contains matches that may be invalidated by a future negative event
        if the timestamp has passed, they can't be invalidated anymore,
        therefore we remove them from waiting for timeout
        and we put them in the field "matches to handle at eof" of the root,
        for it to put it in the matches at the end of the program
        """

        if self.is_last:
            self.waiting_for_time_out = sorted(self.waiting_for_time_out, key=lambda x: x.first_timestamp)
            count = find_partial_match_by_timestamp(self.waiting_for_time_out, last_timestamp - self._sliding_window)
            node = self.get_root()
            node.matches_to_handle_at_EOF.extend(self.waiting_for_time_out[:count])
            self.waiting_for_time_out = self.waiting_for_time_out[count:]

    def _set_event_definitions(self,
                               left_event_defs: List[Tuple[int, QItem]], right_event_defs: List[Tuple[int, QItem]]):
        self._event_defs = merge(left_event_defs, right_event_defs, key=lambda x: x[0])

    # In an NegationNode, the event_def represents all the positives events plus the negative event we are currently checking
    def get_event_definitions(self):  # to support multiple neg
        return self._left_subtree.get_event_definitions()

    def _try_create_new_match(self,
                              first_partial_match: PartialMatch, second_partial_match: PartialMatch,
                              first_event_defs: List[Tuple[int, QItem]], second_event_defs: List[Tuple[int, QItem]]):

        if self._sliding_window != timedelta.max and \
                abs(first_partial_match.last_timestamp - second_partial_match.first_timestamp) > self._sliding_window:
            return

        if self.top_operator == SeqOperator:
            events_for_new_match = merge_according_to(first_event_defs, second_event_defs,
                                                      first_partial_match.events, second_partial_match.events,
                                                      key=lambda x: x[0])
            if not is_sorted(events_for_new_match, key=lambda x: x.timestamp):
                return False
        else:
            events_for_new_match = self._merge_events_for_new_match(first_event_defs, second_event_defs,
                                                                    first_partial_match.events,
                                                                    second_partial_match.events)

        return self._validate_new_match(events_for_new_match)

    def handle_new_partial_match(self, partial_match_source: Node):
        if partial_match_source == self._left_subtree:
            other_subtree = self._right_subtree
            if self.is_last:
                new_partial_match = partial_match_source.get_last_unhandled_partial_match()
                self.waiting_for_time_out.append(new_partial_match)
                return
        elif partial_match_source == self._right_subtree:
            if self.is_last:
                self.handle_PM_with_negation_at_the_end(partial_match_source)
            return
        else:
            raise Exception()  # should never happen

        # we arrive here only if the new partial match is a positive event
        new_partial_match = partial_match_source.get_last_unhandled_partial_match()
        first_event_defs = partial_match_source.get_event_definitions()
        other_subtree.clean_expired_partial_matches(new_partial_match.last_timestamp)

        partial_matches_to_compare = other_subtree.get_partial_matches()
        second_event_defs = other_subtree.get_event_definitions()
        self.clean_expired_partial_matches(new_partial_match.last_timestamp)

        for partialMatch in partial_matches_to_compare:
            # for every negative event, we want to check if it invalidates new_partial_match
            if self._try_create_new_match(new_partial_match, partialMatch, first_event_defs, second_event_defs):
                return

        self.add_partial_match(new_partial_match)
        if self._parent is not None:
            self._parent.handle_new_partial_match(self)

    def handle_PM_with_negation_at_the_end(self, partial_match_source: Node):
        """
        Customized handle_new_partial_match function for partial matches waiting for negative events at the end of the pattern:
        The PMs to compare come from "get_waiting_for_timeout" and not from "get_partial_matches": these are PMs that
        match the pattern but may be invalidated by a later negative event at the end of a pattern.
        We check which ones have been invalidated and we discard them. The others will be final matches once there are
        no future not event that can invalidate them == when the time window has ended or EOF
        """

        other_subtree = self.get_first_last_negative_node()

        new_partial_match = partial_match_source.get_last_unhandled_partial_match()
        first_event_defs = partial_match_source.get_event_definitions()
        other_subtree.clean_expired_partial_matches(new_partial_match.last_timestamp)

        partial_matches_to_compare = other_subtree.waiting_for_time_out
        second_event_defs = other_subtree.get_event_definitions()
        self.clean_expired_partial_matches(new_partial_match.last_timestamp)

        matches_to_keep = []
        for partialMatch in partial_matches_to_compare:
            if not self._try_create_new_match(new_partial_match, partialMatch, first_event_defs, second_event_defs):
                matches_to_keep.append(partialMatch)

        other_subtree.waiting_for_time_out = matches_to_keep

    def get_first_last_negative_node(self):
        """
        This function descends in the tree and returns the first Node that is not a NegationNode at the end
        of the Pattern. That's in that node that we keep the PMs that are waiting for timeout: we block them here,
        because if they go directly up to the root they are automatically added to the matches
        """
        if type(self._left_subtree) == NegationNode and self._left_subtree.is_last:
            return self._left_subtree.get_first_last_negative_node()
        else:
            return self

class Tree:
    """
    Represents an evaluation tree. Implements the functionality of constructing an actual tree from a "tree structure"
    object returned by a tree builder. Other than that, merely acts as a proxy to the tree root node.
    """
    def __init__(self, tree_structure: tuple, pattern: Pattern):
        # Note that right now only "flat" sequence patterns and "flat" conjunction patterns are supported
        self.__root = Tree.__construct_tree(pattern.structure.get_top_operator() == SeqOperator,
                                            tree_structure, pattern.structure.args, pattern.window)

        self.__root.apply_formula(pattern.condition)

        if pattern.negative_event.get_args():
            self.reorder_leaf_index(pattern)
            root = self.__root
            self.__root = self.create_negation_Tree(root, pattern)

    def reorder_leaf_index(self, pattern: Pattern):
        """
        Fix the values of the index in event_def according to the "true" position of the QItem in the original structure
        """
        leaves = self.get_leaves()
        for i in range(len(leaves)):
            leaf = leaves[i]
            leaf_name = leaf.get_event_name()
            index = pattern.find_index_from_name(leaf_name)
            if index != leaf.get_leaf_index():
                leaf.set_leaf_index(index)
                if leaf._parent is not None:
                    leaf._parent.fix_eventdef(index, leaf_name)
                else:
                    raise Exception("Leaf without a parent")

    def create_negation_Tree(self, root: Node, pattern: Pattern):
        """
        We add the negative nodes at the root of the tree
        """
        top_operator = pattern.origin_structure.get_top_operator()
        negative_event_list = pattern.negative_event.get_args()
        # contains only not operators
        origin_event_list = pattern.origin_structure.get_args()
        # contains the original pattern with all operators

        for p in negative_event_list:
            if len(negative_event_list) - negative_event_list.index(p) \
                    == len(origin_event_list) - origin_event_list.index(p):
                temporal_root = NegationNode(pattern.window, is_last=True, top_operator=top_operator)
            else:
                temporal_root = NegationNode(pattern.window, is_last=False, top_operator=top_operator)

            qitem = p.get_args()
            index = pattern.find_index_from_name(qitem.get_event_name())
            neg_event = LeafNode(pattern.window, index, qitem, temporal_root)
            temporal_root.set_subtrees(root, neg_event)
            neg_event.set_parent(temporal_root)
            root.set_parent(temporal_root)
            root = root._parent

            # apply_formula manually for negation nodes
            names = {item[1].name for item in root._event_defs}
            condition = pattern.condition.get_formula_of(names)
            root._condition = condition if condition else TrueFormula()
            neg_event.apply_formula(pattern.condition)

        self.__root = root
        return self.__root

    def get_leaves(self):
        return self.__root.get_leaves()

    def get_matches(self):
        while self.__root.has_partial_matches():
            yield self.__root.consume_first_partial_match().events

    def get_root(self):
        return self.__root

    def handle_EOF(self, matches: Stream):
        """
        We add as matches all the PMs for which there was a risk to be invalidated later.
        Now we finished the input stream so there is no more risk !
        """
        for match in self.__root.matches_to_handle_at_EOF:
            matches.add_item(PatternMatch(match.events))
        node = self.__root.get_first_last_negative_node()
        for match in node.waiting_for_time_out:
            matches.add_item(PatternMatch(match.events))

    @staticmethod
    def __construct_tree(is_sequence: bool, tree_structure: tuple or int, args: List[QItem],
                         sliding_window: timedelta, parent: Node = None):
        if type(tree_structure) == int:
            return LeafNode(sliding_window, tree_structure, args[tree_structure], parent)
        current = SeqNode(sliding_window, parent) if is_sequence else AndNode(sliding_window, parent)
        left_structure, right_structure = tree_structure
        left = Tree.__construct_tree(is_sequence, left_structure, args, sliding_window, current)
        right = Tree.__construct_tree(is_sequence, right_structure, args, sliding_window, current)

        current.set_subtrees(left, right)
        return current


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
            if event.event_type in event_types_listeners.keys():
                for leaf in event_types_listeners[event.event_type]:
                    leaf.handle_event(event)
                    for match in self.__tree.get_matches():
                        matches.add_item(PatternMatch(match))

        # Now that we finished the input stream, if there were some PMs risking to be invalidated by a negative event
        # at the end of the pattern, we handle them now
        if type(self.__tree.get_root()) == NegationNode and self.__tree.get_root().is_last:
            self.__tree.handle_EOF(matches)

        matches.close()
