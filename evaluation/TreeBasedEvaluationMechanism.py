from abc import ABC
from datetime import timedelta, datetime
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, QItem
from base.Formula import TrueFormula, Formula
from evaluation.PartialMatch import PartialMatch
from misc.IOUtils import Stream
from typing import List, Tuple
from base.Event import Event
from misc.Utils import merge, merge_according_to, is_sorted, find_partial_match_by_timestamp
from base.PatternMatch import PatternMatch
from evaluation.EvaluationMechanism import EvaluationMechanism
from queue import Queue
from statisticsCollector.StatisticsCollector import StatisticsCollector
from enum import Enum


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
        if count != 0 and type(self) == LeafNode:
            if self.statistics_collector is not None:
                for i in range(count):
                    self.statistics_collector.remove_event(self._partial_matches[i].events[0])
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
    def __init__(self, sliding_window: timedelta, leaf_index: int, leaf_qitem: QItem, parent: Node,
                 statistics_collector: StatisticsCollector):
        super().__init__(sliding_window, parent)
        self.__leaf_index = leaf_index
        self.__event_name = leaf_qitem.name
        self.__event_type = leaf_qitem.event_type
        self.statistics_collector = statistics_collector

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


class Tree:
    """
    Represents an evaluation tree. Implements the functionality of constructing an actual tree from a "tree structure"
    object returned by a tree builder. Other than that, merely acts as a proxy to the tree root node.
    """
    def __init__(self, tree_structure: tuple, pattern: Pattern, statistics_collector: StatisticsCollector):
        # Note that right now only "flat" sequence patterns and "flat" conjunction patterns are supported
        self.__root = Tree.__construct_tree(pattern.structure.get_top_operator() == SeqOperator,
                                            tree_structure, pattern.structure.args, pattern.window,
                                            statistics_collector)
        self.__root.apply_formula(pattern.condition)

    def get_leaves(self):
        return self.__root.get_leaves()

    def get_matches(self):
        while self.__root.has_partial_matches():
            yield self.__root.consume_first_partial_match().events

    @staticmethod
    def __construct_tree(is_sequence: bool, tree_structure: tuple or int, args: List[QItem],
                         sliding_window: timedelta, statistics_collector: StatisticsCollector,
                         parent: Node = None):
        if type(tree_structure) == int:
            return LeafNode(sliding_window, tree_structure, args[tree_structure], parent, statistics_collector)
        current = SeqNode(sliding_window, parent) if is_sequence else AndNode(sliding_window, parent)
        left_structure, right_structure = tree_structure
        left = Tree.__construct_tree(is_sequence, left_structure, args, sliding_window, statistics_collector, current)
        right = Tree.__construct_tree(is_sequence, right_structure, args, sliding_window, statistics_collector, current)
        current.set_subtrees(left, right)
        return current


class TreeReplacementAlgorithmTypes(Enum):
    IMMEDIATE_REPLACE_TREE = 0,
    SIMULTANEOUSLY_RUN_TWO_TREES = 1,
    COMMON_PARTS_SHARE = 2


class TreeBasedEvaluationMechanism(EvaluationMechanism):
    """
    An implementation of the tree-based evaluation mechanism.
    """
    def __init__(self, pattern: Pattern, tree_structure: tuple, statistics_collector: StatisticsCollector):
        self.__tree = Tree(tree_structure, pattern, statistics_collector)
        self.pattern = pattern
        self.event_types_listeners = self.find_event_types_listeners(self.__tree)
        self.active_events = Stream()  # A stream of the current events
        self.__tree2 = None
        self.event_types_listeners2 = None
        self.tree2_create_time = None

    @staticmethod
    def find_event_types_listeners(tree: Tree):
        """
        Looking for all type of events that are in the leaves of the tree
        """
        event_types_listeners = {}
        # register leaf listeners for event types.
        for leaf in tree.get_leaves():
            event_type = leaf.get_event_type()
            if event_type in event_types_listeners.keys():
                event_types_listeners[event_type].append(leaf)
            else:
                event_types_listeners[event_type] = [leaf]
        return event_types_listeners

    def eval(self, events: Stream, matches: Stream, statistics_collector: StatisticsCollector, optimizer,
             eval_mechanism_params):
        """
        Evaluating the events from the stream.
        Using the Statistics Collector and Optimizer if CEP runs on adaptive mode, otherwise using basic evaluation
        """
        adaptive_parameters = None if eval_mechanism_params is None else eval_mechanism_params.adaptive_parameters
        if adaptive_parameters is None:     # Meaning CEP is not in adaptive mode
            self.eval_not_adaptive(events, matches)
        else:
            last_activated_statistics_collector_period = last_activated_optimizer_period = None
            stat = statistics_collector.get_stat()
            for event in events:
                if last_activated_statistics_collector_period is None:
                    last_activated_statistics_collector_period = last_activated_optimizer_period = event.timestamp
                if event.timestamp - last_activated_statistics_collector_period > \
                        adaptive_parameters.activate_statistics_collector_period:
                    stat = statistics_collector.get_stat()
                    last_activated_statistics_collector_period = event.timestamp
                if event.timestamp - last_activated_optimizer_period > \
                        adaptive_parameters.activate_optimizer_period:
                    new_plan = optimizer.run(stat)
                    last_activated_optimizer_period = event.timestamp
                else:
                    new_plan = None
                if adaptive_parameters.tree_replacement_algorithm_type == TreeReplacementAlgorithmTypes.IMMEDIATE_REPLACE_TREE:
                    self.eval_immediate_replace_tree(event, new_plan, matches, statistics_collector)
                elif adaptive_parameters.tree_replacement_algorithm_type == TreeReplacementAlgorithmTypes.SIMULTANEOUSLY_RUN_TWO_TREES:
                    self.eval_simultaneously_run_two_trees(event, new_plan, matches, statistics_collector)
                else:
                    raise NotImplementedError()
            matches.close()

    def eval_not_adaptive(self, events: Stream, matches: Stream):
        """
        Basic non-adaptive evaluation
        """
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

    def eval_immediate_replace_tree(self, event: Event, new_order_for_tree, matches: Stream,
                                    statistics_collector: StatisticsCollector):
        """
        Immediately replace the old tree with a new tree.
        Building the new tree according to the new plan from the Optimizer and inserts all the relevant events
        in the stream to the new tree.
        """
        self.__remove_expired_events(event.timestamp)
        if new_order_for_tree is not None:
            new_tree = Tree(new_order_for_tree, self.pattern, statistics_collector)
            self.__update_current_events_in_new_tree(new_tree)
        # Send events to listening leaves.
        if event.event_type in self.event_types_listeners.keys():
            statistics_collector.insert_event(event)
            for leaf in self.event_types_listeners[event.event_type]:
                leaf.handle_event(event)
                for match in self.__tree.get_matches():
                    matches.add_item(PatternMatch(match))
        self.active_events.add_item(event)

    def eval_simultaneously_run_two_trees(self, event: Event, new_order_for_tree, matches: Stream,
                                          statistics_collector: StatisticsCollector):
        """
        Simultaneously run the previous tree with a new tree that is build according to a new plan from the Optimizer
        Removes the old tree when the time window expired and replaced by the new tree
        """
        temp_tree1_matches = []
        if self.__tree2 is not None:
            # Checking if tree 1 should be replaced with tree 2 because the time window expired
            if event.timestamp - self.__tree2_create_time > self.pattern.window:
                # If replacement is needed then initialize tree1 with tree2 data
                self.__tree = self.__tree2
                self.event_types_listeners = self.event_types_listeners2
                self.__tree2 = None
                self.event_types_listeners2 = None
                self.__tree2_create_time = None
        # Checking if a new plan was received from Optimizer
        if new_order_for_tree is not None:
            new_tree = Tree(new_order_for_tree, self.pattern, statistics_collector)
            self.__tree2 = new_tree
            self.event_types_listeners2 = self.find_event_types_listeners(self.__tree2)
            self.__tree2_create_time = event.timestamp
        # Insert and handles the incoming event in tree1
        if event.event_type in self.event_types_listeners.keys():
            statistics_collector.insert_event(event)
            for leaf in self.event_types_listeners[event.event_type]:
                leaf.handle_event(event)
                for match in self.__tree.get_matches():
                    matches.add_item(PatternMatch(match))
                    temp_tree1_matches.append(match)
        if self.__tree2 is not None:
            # Insert and handles the incoming event in tree2
            if event.event_type in self.event_types_listeners2.keys():
                for leaf in self.event_types_listeners2[event.event_type]:
                    leaf.handle_event(event)
                for match in self.__tree2.get_matches():
                    match_exists = False
                    for tree1_match in temp_tree1_matches:
                        if compare_matches(match, tree1_match):
                            match_exists = True
                            break
                    if match_exists is False:
                        matches.add_item(PatternMatch(match))
        temp_tree1_matches.clear()

    def __remove_expired_events(self, curr_time: datetime):
        """
            Removing out of date events from the stream active_events
        """
        while self.active_events.count() != 0:
            last_event = self.active_events.first()
            if curr_time - last_event.timestamp > self.pattern.window:
                self.active_events.get_item()
            else:
                return

    def __update_current_events_in_new_tree(self, new_tree):
        """
            Inserting the events from the active_events stream into the new tree to be replaced
            and handling the partial matches in it
        """
        if self.active_events.count() == 0:  # Important because iteration over an empty stream is not possible
            return
        self.__tree = new_tree
        self.event_types_listeners = self.find_event_types_listeners(self.__tree)
        temp_active_events = self.active_events.duplicate()
        for event in temp_active_events:
            if event.event_type in self.event_types_listeners.keys():
                for leaf in self.event_types_listeners[event.event_type]:
                    leaf.handle_event(event)
            if temp_active_events.count() == 0:
                for __ in self.__tree.get_matches():      # an empty iteration to clean the partial matches in the root
                    pass
                return
        return


def compare_matches(match1: List[Event], match2: List[Event]):
    """
    Compare 2 matches according to their events
    """
    if len(match1) != len(match2):
        return False
    for event1 in match1:
        if event1 not in match2:
            return False
    return True
