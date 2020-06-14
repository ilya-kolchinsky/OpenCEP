from abc import ABC
from datetime import timedelta, datetime
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, QItem, NegationOperator
from base.Formula import TrueFormula, Formula
from evaluation.PartialMatch import PartialMatch
from misc.IOUtils import Stream
from typing import List, Tuple
from base.Event import Event
from misc.Utils import merge, merge_according_to, is_sorted, find_partial_match_by_timestamp, get_index
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

    def clean_expired_partial_matches(self, last_timestamp: datetime):
        """
        Removes partial matches whose earliest timestamp violates the time window constraint.
        """
        if self._sliding_window == timedelta.max:
            return
        count = find_partial_match_by_timestamp(self._partial_matches, last_timestamp - self._sliding_window)
        self._partial_matches = self._partial_matches[count:]

        if type(self) == InternalNegationNode and self.is_last:
            count = find_partial_match_by_timestamp(self._waiting_for_time_out, last_timestamp - self._sliding_window)
            node = self
            while node._parent is not None:
                node = node._parent
            node.matches_to_handle_at_EOF.extend(self._waiting_for_time_out[:count])
            self._waiting_for_time_out = self._waiting_for_time_out[count:]

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
        #EVA
        self.qitem_index = leaf_qitem.get_event_index()

    def get_leaves(self):
        return [self]

    def set_qitem_index(self, index: int):
        self.qitem_index = index

    def apply_formula(self, formula: Formula):
        condition = formula.get_formula_of(self.__event_name)
        if condition is not None:
            self._condition = condition

    def get_event_definitions(self):
        return [(self.__leaf_index, QItem(self.__event_type, self.__event_name, self.qitem_index))]

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

    def get_event_name(self):
        """
        Returns the name of the event processed by this leaf.
        """
        return self.__event_name

class InternalNode(Node):
    """
    An internal node connects two subtrees, i.e., two subpatterns of the evaluated pattern.
    """
    def __init__(self, sliding_window: timedelta, parent: Node = None, event_defs: List[Tuple[int, QItem]] = None,
                 left: Node = None, right: Node = None):
        super().__init__(sliding_window, parent)
        self._event_defs = event_defs #nathanb: contains all the QItem present in this subtree
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

    """
    def sort_event_def_according_to(self, pattern: Pattern):
        if(type(self) != InternalNegationNode):
            return
        pattern_list = pattern.origin_structure.get_args()
        qitem_list = list(list(zip(*self._event_defs))[1])

        qitem_list_name = [x.name for x in qitem_list]
        
        for name in qitem_list_name:
            if name == self._right_subtree.__event_name:
                index_to_delete = qitem_list_name.index(name)
                tuple_to_insert = self._event_defs.pop(index_to_delete)
                break

        for p in pattern_list:
            if p.name == name:
                new_index = pattern_list.index(p)

        list_name = [x.name for x in list_for_sorting]
        

        sorted_qitems = sorted(qitem_list, key=lambda x: x.get_event_index())
        """


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


class InternalNegationNode(InternalNode):
    """
    An internal node connects two subtrees, i.e., two subpatterns of the evaluated pattern.
    """

    def __init__(self, sliding_window: timedelta, is_first: bool, is_last: bool, parent: Node = None, event_defs: List[Tuple[int, QItem]] = None,
                 left: Node = None, right: Node = None):
        super().__init__(sliding_window, parent)
        self.is_first = is_first
        self.is_last = is_last
        self._waiting_for_time_out = []
        self.matches_to_handle_at_EOF = []


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

    #14.06: comment ça marche ? - nathan
    def get_event_definitions(self):#to support multiple neg
        if type(self._left_subtree) == InternalNegationNode:
            return self._left_subtree.get_event_definitions()
        else:
            return self._left_subtree._event_defs

    def _try_create_new_match(self,
                              first_partial_match: PartialMatch, second_partial_match: PartialMatch,
                              first_event_defs: List[Tuple[int, QItem]], second_event_defs: List[Tuple[int, QItem]]):
        
        if self._sliding_window != timedelta.max and \
                abs(first_partial_match.last_timestamp - second_partial_match.first_timestamp) > self._sliding_window:
            return

        #12/16 Ici faire juste merge_according_to avec la fonction get_index en key et tester
        """
        if self.is_first:
            events_for_new_match = second_partial_match.events + first_partial_match.events
        elif self.is_last:
            events_for_new_match = first_partial_match.events + second_partial_match.events
        else:
            events_for_new_match = self._merge_events_for_new_match(first_event_defs, second_event_defs,
                                                                first_partial_match.events, second_partial_match.events)
        """
        events_for_new_match = merge_according_to(first_event_defs, second_event_defs,
                                                  first_partial_match.events, second_partial_match.events, key=get_index)

        if not is_sorted(events_for_new_match, key=lambda x: x.timestamp):
            return False

        return self._validate_new_match(events_for_new_match)
        # self._remove_partial_match(events_for_new_match)

    def _validate_new_match(self, events_for_new_match: List[Event]):
        """
        Validates the condition stored in this node on the given set of events.
        """
        binding = {
            self._event_defs[i][1].name: events_for_new_match[i].payload for i in range(len(self._event_defs))
        }
        return self._condition.eval(binding)

    """
        if type(self._left_subtree) != LeafNode:
            return self._left_subtree._validate_new_match(events_for_new_match)
        else:
            return super()._validate_new_match(events_for_new_match)

    def _remove_partial_match(self, match_to_remove: List[Event]):
        i = 0
        m = set(match_to_remove)
        while i < len(self._left_subtree._partial_matches):
            n = set(self._left_subtree._partial_matches[i].events)
            if n.issubset(set(m)):
                self._left_subtree._partial_matches.pop(i)
            i += 1
        #self._left_subtree._partial_matches = [x for x in self._left_subtree._partial_matches.event if not set(x).issubset(m)]
    """
    def get_waiting_for_time_out(self):
        if type(self._left_subtree) == InternalNegationNode and self._left_subtree.is_last:
            return self._left_subtree.get_waiting_for_time_out()
        else:
            return self._waiting_for_time_out

    def get_first_last_negative_node(self):
        if type(self._left_subtree) == InternalNegationNode and self._left_subtree.is_last:
            return self._left_subtree.get_first_last_negative_node()
        else:
            return self

    def handle_PM_with_negation_at_the_end(self, partial_match_source: Node):
        #if type(self._left_subtree) != InternalNegationNode:


        other_subtree = self.get_first_last_negative_node()


        new_partial_match = partial_match_source.get_last_unhandled_partial_match()  #C
        first_event_defs = partial_match_source.get_event_definitions()
        other_subtree.clean_expired_partial_matches(new_partial_match.last_timestamp)

        partial_matches_to_compare = self.get_waiting_for_time_out()
        second_event_defs = other_subtree.get_event_definitions()
        self.clean_expired_partial_matches(new_partial_match.last_timestamp)

        for partialMatch in partial_matches_to_compare:  # pour chaque pm qu'on a "bloqué" on verifie si le nouveau event not va invalider
            if self._try_create_new_match(new_partial_match, partialMatch, first_event_defs, second_event_defs):
                other_subtree._waiting_for_time_out.remove(partialMatch)

    def handle_new_partial_match(self, partial_match_source: Node):

        if partial_match_source == self._left_subtree:
            other_subtree = self._right_subtree
            if self.is_last:
                new_partial_match = partial_match_source.get_last_unhandled_partial_match()  # A1 et C1
                self._waiting_for_time_out.append(new_partial_match)
                return

        elif partial_match_source == self._right_subtree:
            if self.is_last:
                self.handle_PM_with_negation_at_the_end(partial_match_source)
            return
            # si on vient de rajouter un QItem qui est NOT, on ne veut rien faire avec,
            # on ne veut ni le faire monter en tant que partial match ni comparer avec les autres.
        else:
            raise Exception()  # should never happen

        new_partial_match = partial_match_source.get_last_unhandled_partial_match()  # A1 et C1
        first_event_defs = partial_match_source.get_event_definitions()
        other_subtree.clean_expired_partial_matches(new_partial_match.last_timestamp)

        partial_matches_to_compare = other_subtree.get_partial_matches()  # B
        second_event_defs = other_subtree.get_event_definitions()
        self.clean_expired_partial_matches(new_partial_match.last_timestamp)

        for partialMatch in partial_matches_to_compare:  # pour chaque negation object, on veut verifier si il n'invalide pas new_partial_match
            if self._try_create_new_match(new_partial_match, partialMatch, first_event_defs, second_event_defs):
                return
                # self._remove_partial_match(new_partial_match)

        self.add_partial_match(new_partial_match)
        if self._parent is not None:
            self._parent.handle_new_partial_match(self)


class Tree:
    """
    Represents an evaluation tree. Implements the functionality of constructing an actual tree from a "tree structure"
    object returned by a tree builder. Other than that, merely acts as a proxy to the tree root node.
    """
    def __init__(self, tree_structure: tuple, pattern: Pattern):
        # Note that right now only "flat" sequence patterns and "flat" conjunction patterns are supported

        temp_root = Tree.__construct_tree(pattern.structure.get_top_operator() == SeqOperator,
                                            tree_structure, pattern.structure.args, pattern.window)
        temp_root.apply_formula(pattern.condition)

        negative_event_list = pattern.negative_event.get_args()
        origin_event_list = pattern.origin_structure.get_args()
        #root = InternalNode(pattern.window)
        #temp_root = positive_root

        # 14.06 : comment sont fixés is first et is last ? comment se servir de is last pour un not a la fin ? - nathan
        counter = 0
        for p in negative_event_list:
            if p == origin_event_list[counter]:
                temporal_root = InternalNegationNode(pattern.window, is_first=True, is_last=False)
                counter+=1
            elif len(negative_event_list) - negative_event_list.index(p) == len(origin_event_list) - origin_event_list.index(p):
                temporal_root = InternalNegationNode(pattern.window, is_first=False, is_last=True)
            else:
                temporal_root = InternalNegationNode(pattern.window, is_first=False, is_last=False)

            temp_neg_event = LeafNode(pattern.window, 1, p, temporal_root)
            temporal_root.set_subtrees(temp_root, temp_neg_event)
            temp_neg_event.set_parent(temporal_root)
            temp_root.set_parent(temporal_root)
            temp_root = temp_root._parent

            names = {item[1].name for item in temp_root._event_defs}
            condition = pattern.condition.get_formula_of(names)
            temp_root._condition = condition if condition else TrueFormula()

        self.__root = temp_root


        self.reorder_event_def(pattern)

        #self.__root.apply_formula(pattern.condition)

    def get_root(self):
        return self.__root

    def reorder_event_def(self, pattern: Pattern):
        """
        leaf_list = self.get_leaves()
        for leaf in leaf_list:
            for p in pattern.origin_structure.get_args():
                if p.get_event_name() == leaf.get_event_name(): leaf.set_qitem_index(p.get_event_index())
        """
        current_node = self.__root
        while type(current_node) != LeafNode:
            if (type(current_node) == InternalNegationNode):
                current_node._event_defs.sort(key=get_index)
            current_node = current_node._left_subtree


    def handle_EOF(self , matches: Stream):
        for match in self.__root.matches_to_handle_at_EOF:
            matches.add_item(PatternMatch(match.events))
        node = self.__root.get_first_last_negative_node()
        for match in node._waiting_for_time_out:
            matches.add_item(PatternMatch(match.events))

    def get_leaves(self):
        return self.__root.get_leaves()

    def get_matches(self):
        while self.__root.has_partial_matches():
            yield self.__root.consume_first_partial_match().events

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

        if type(self.__tree.get_root()) == InternalNegationNode and self.__tree.get_root().is_last:
            self.__tree.handle_EOF(matches)

        matches.close()
