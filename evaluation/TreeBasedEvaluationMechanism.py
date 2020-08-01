from abc import ABC
from datetime import timedelta, datetime
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, QItem
from base.Formula import TrueFormula, Formula
# from evaluation.LeftDeepTreeBuilders import LeftDeepTreeBuilder
from evaluation.PartialMatch import PartialMatch
from misc.IOUtils import Stream
from typing import List, Tuple
from base.Event import Event
from misc.Utils import merge, merge_according_to, is_sorted, find_partial_match_by_timestamp, powerset_generator
from base.PatternMatch import PatternMatch
from base.PatternStructure import *
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


class InternalNode(Node, ABC):
    def __init__(self, sliding_window: timedelta, parent: Node = None, event_defs: List[Tuple[int, QItem]] = None):
        super().__init__(sliding_window, parent)
        self._event_defs = event_defs

    def get_event_definitions(self):
        return self._event_defs

    def _validate_new_match(self, events_for_new_match: List[Event]):
        """
        Validates the condition stored in this node on the given set of events.
        """
        binding = {
            self._event_defs[i][1].name: events_for_new_match[i].payload for i in range(len(self._event_defs))
        }
        return self._condition.eval(binding)

    def get_leaves(self):
        raise NotImplementedError()

    def apply_formula(self, formula: Formula):
        raise NotImplementedError()


class UnaryNode(InternalNode, ABC):

    def __init__(self, sliding_window: timedelta, parent: Node = None, child: Node = None):
        super().__init__(sliding_window, parent)
        self._child = child

    def get_leaves(self):
        result = []
        if self._child is None:
            raise Exception("Unary Node with no child")

        result += self._child.get_leaves()
        return result

    def _set_event_definitions(self, event_defs: List[Tuple[int, QItem]]):
        self._event_defs = event_defs

    def apply_formula(self, formula: Formula):
        names = {item[1].name for item in self._event_defs}
        condition = formula.get_formula_of(names)
        self._condition = condition if condition else TrueFormula()
        self._child.apply_formula(self._condition)

    def set_subtrees(self, child: Node):
        self._child = child
        self._set_event_definitions(self._child.get_event_definitions())

    def handle_new_partial_match(self, partial_match_source: Node):
        raise NotImplementedError()


class BinaryNode(InternalNode, ABC):
    """
    An internal node connects two subtrees, i.e., two subpatterns of the evaluated pattern.
    """
    def __init__(self, sliding_window: timedelta, parent: Node = None, left: Node = None, right: Node = None):
        super().__init__(sliding_window, parent)
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

    def _set_event_definitions(self,
                               left_event_defs: List[Tuple[int, QItem]], right_event_defs: List[Tuple[int, QItem]]):
        """
        A helper function for collecting the event definitions from subtrees. To be overridden by subclasses.
        """
        raise NotImplementedError()

    def set_subtrees(self, left: Node, right: Node):
        """
        Sets the subtrees of this node.
        """
        self._left_subtree = left
        self._right_subtree = right
        self._set_event_definitions(self._left_subtree.get_event_definitions(),
                                    self._right_subtree.get_event_definitions())

    def handle_new_partial_match(self, partial_match_source: Node):
        raise NotImplementedError()


class AndNode(BinaryNode):
    """
    An internal node representing an "AND" operator.
    """
    def _set_event_definitions(self,
                               left_event_defs: List[Tuple[int, QItem]], right_event_defs: List[Tuple[int, QItem]]):
        """
        A helper function for collecting the event definitions from subtrees. To be overridden by subclasses.
        """
        self._event_defs = left_event_defs + right_event_defs

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


class SeqNode(BinaryNode):
    """
    An internal node representing a "SEQ" (sequence) operator.
    In addition to checking the time window and condition like the basic node does, SeqNode also verifies the order
    of arrival of the events in the partial matches it constructs.
    """
    def _set_event_definitions(self, left_event_defs: List[Tuple[int, QItem]], right_event_defs: List[Tuple[int, QItem]]):
        self._event_defs = merge(left_event_defs, right_event_defs, key=lambda x: x[0])

    def _merge_events_for_new_match(self,
                                    first_event_defs: List[Tuple[int, QItem]],
                                    second_event_defs: List[Tuple[int, QItem]],
                                    first_event_list: List[Event],
                                    second_event_list: List[Event]):
        return merge_according_to(first_event_defs, second_event_defs,
                                  first_event_list, second_event_list, key=lambda x: x[0])

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

    def _validate_new_match(self, events_for_new_match: List[Event]):
        if not is_sorted(events_for_new_match, key=lambda x: x.timestamp):
            return False
        return super()._validate_new_match(events_for_new_match)

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


class KleeneClosureNode(UnaryNode):
    """
    An internal node representing a "KC" (KleeneClosure) operator.
    In addition to checking the time window and condition like the basic node does, KleeneClosureNode also verifies that
    no duplicated events are sent and filters matches based on previously failed prefixes (PREFIX NOT YET IMPLEMENTED).
    """
    def __init__(self, sliding_window: timedelta, min_size, max_size, parent: Node = None):
        super().__init__(sliding_window, parent)
        self._min_size = min_size
        self._max_size = max_size

    def partial_match_from_partial_match_set(self, power_match):
        min_timestamp = None
        max_timestamp = None

        cur_event = []

        for match in power_match:
            min_timestamp = match.first_timestamp if not min_timestamp else min(min_timestamp, match.first_timestamp)
            max_timestamp = match.last_timestamp if not max_timestamp else max(max_timestamp, match.last_timestamp)
            cur_event.extend(match.events)

        return PartialMatch(cur_event)

    def _try_create_new_match(self, new_partial_match: PartialMatch, power_match: PartialMatch,
                              event_defs: List[Tuple[int, QItem]]):
        """
        Gather all satisfied conditions and create a new partial match for every new item in the events powerset
        """

        if power_match is None:
            return

        if self._sliding_window != timedelta.max and \
                abs(new_partial_match.last_timestamp - power_match.first_timestamp) > self._sliding_window:
            return

        events_for_new_match = power_match.events

        # forward partial match to parent if exists or save in current node if this is the root node.
        # very important for cases when KC node is the root node.
        if self._parent is not None:
            self._parent.handle_new_partial_match(self)
        else:
            self.add_partial_match(PartialMatch(events_for_new_match))

    def handle_new_partial_match(self, partial_match_source: Node):
        if self._child is None:
            raise Exception()  # should never happen

        new_partial_match = self._child.get_last_unhandled_partial_match()
        event_defs = self._child.get_event_definitions()

        self._child.clean_expired_partial_matches(new_partial_match.last_timestamp)

        # generates a list of all subsets (using a generator).
        child_partial_matches = self._child.get_partial_matches()
        # create child event power-set
        child_matches_powerset = powerset_generator(child_partial_matches, self._min_size, self._max_size)

        for partialMatch in child_matches_powerset:
            partial_match = self.partial_match_from_partial_match_set(partialMatch)
            self._try_create_new_match(new_partial_match, partial_match, event_defs)


class Tree:
    """
    Represents an evaluation tree. Implements the functionality of constructing an actual tree from a "tree structure"
    object returned by a tree builder. Other than that, merely acts as a proxy to the tree root node.
    """
    def __init__(self, tree_structure: tuple, pattern: Pattern):
        self.__current_leaf_number = 0
        self.__root = self.__construct_tree(pattern.structure, tree_structure, pattern.structure.args, pattern.window)
        self.__root.apply_formula(pattern.condition)

    def get_leaves(self):
        return self.__root.get_leaves()

    def get_matches(self):
        while self.__root.has_partial_matches():
            yield self.__root.consume_first_partial_match().events

    @staticmethod
    def __generate_new_node(node_type, sliding_window, parent, min_size=1, max_size=5):
        if node_type == SeqOperator:
            return SeqNode(sliding_window, parent)
        elif node_type == AndOperator:
            return AndNode(sliding_window, parent)
        elif node_type == KleeneClosureOperator:
            return KleeneClosureNode(sliding_window, min_size, max_size, parent)
        elif node_type == OrOperator:
            raise Exception("Not Yet Implemented")
        elif node_type == NegationOperator:
            raise Exception("Not Yet Implemented")
        else:
            raise Exception("Unknown Operator discovered.")

    def __construct_tree(self, root_operator, tree_structure: tuple or int, args: List[PatternStructure],
                         sliding_window: timedelta, parent: Node = None):
        root_type = root_operator.get_top_operator()
        # tree_structure is int when we need to build current nested node or leaf node, or when Unary operator arrives
        if type(tree_structure) == int:
            # QItem found - NO NESTED operations needed.
            if args[tree_structure].get_top_operator() == QItem:
                if root_type == KleeneClosureOperator and not isinstance(parent, KleeneClosureNode):
                    # handling KC child -- create the KC node and the child node, connect child and parent to KC node.
                    current = self.__generate_new_node(KleeneClosureOperator, sliding_window, parent,
                                                       root_operator.min_size, root_operator.max_size)
                    child = LeafNode(sliding_window, self.__current_leaf_number, args[tree_structure], current)
                    self.__current_leaf_number += 1
                    current.set_subtrees(child)
                    return current

                # no KC operations needed or KC node was already generated in previous nesting level -- create the leaf.
                leaf = LeafNode(sliding_window, self.__current_leaf_number, args[tree_structure], parent)
                self.__current_leaf_number += 1
                return leaf

            # NESTED operator found -- recursively calling construct tree
            # nested KC operator -- create KC operator, create subtree recursively and connect KC to child and parent.
            if args[tree_structure].get_top_operator() == KleeneClosureOperator:
                current = self.__generate_new_node(KleeneClosureOperator, sliding_window, parent,
                                                   args[tree_structure].min_size, args[tree_structure].max_size)
                nested_evaluation_order = self._create_evaluation_order(args[tree_structure])
                nested_tree_structure = self.__build_tree_from_order(nested_evaluation_order)
                child = self.__construct_tree(args[tree_structure], nested_tree_structure,
                                              args[tree_structure].args, sliding_window, current)
                current.set_subtrees(child)  # Unary node
                return current
            # nested operator - NOT KC.
            nested_evaluation_order = self._create_evaluation_order(args[tree_structure])
            nested_tree_structure = self.__build_tree_from_order(nested_evaluation_order)
            # create wrapper KC node when parent is none and root is KC operator
            if root_type == KleeneClosureOperator and parent is None:
                current = self.__generate_new_node(KleeneClosureOperator, sliding_window, parent,
                                                   args[tree_structure].min_size, args[tree_structure].max_size)
                child = self.__construct_tree(args[tree_structure], nested_tree_structure,
                                              args[tree_structure].args, sliding_window, current)
                current.set_subtrees(child)
                return current

            return self.__construct_tree(args[tree_structure], nested_tree_structure,
                                         args[tree_structure].args, sliding_window, parent)
        # continue creating nodes based on parent node for every item that still has tree_structure as tuple.
        # this means we do not need to build this node yet, but rather keep building the tree infrastructure.
        # NOTE: operators with 1 argument will NEVER get here, as they meet the condition type(tree_structure) == int.
        current = self.__generate_new_node(root_type, sliding_window, parent)

        left_structure, right_structure = tree_structure
        left = self.__construct_tree(root_operator, left_structure, args, sliding_window, current)
        right = self.__construct_tree(root_operator, right_structure, args, sliding_window, current)
        current.set_subtrees(left, right)
        return current

    @staticmethod
    def _create_evaluation_order(pattern_structure):
        if isinstance(pattern_structure, QItem):
            return [0]
        args_num = len(pattern_structure.args)
        return list(range(args_num))

    @staticmethod
    def __build_tree_from_order(order: List[int]):
        """
        Builds a left-deep tree structure from a given order.
        """
        ret = order[0]
        for i in range(1, len(order)):
            ret = (ret, order[i])
        return ret


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

        matches.close()
