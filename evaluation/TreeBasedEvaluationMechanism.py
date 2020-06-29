from abc import ABC
from datetime import timedelta, datetime
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, QItem, NegationOperator
from base.Formula import TrueFormula, Formula
from evaluation.PartialMatch import PartialMatch
from misc.IOUtils import Stream
from typing import List, Tuple
from base.Event import Event
from misc.Utils import merge, merge_according_to, is_sorted, find_partial_match_by_timestamp, get_index, \
    find_positive_events_before
from base.PatternMatch import PatternMatch
from evaluation.EvaluationMechanism import EvaluationMechanism
from queue import Queue

# check_expired_timestamp = set()

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

        if (type(self) == PostProcessingNode or type(self) == FirstChanceNode) \
                and self.is_last:
            count = find_partial_match_by_timestamp(self._waiting_for_time_out, last_timestamp - self._sliding_window)
            node = self
            while node._parent is not None:
                node = node._parent
            node.matches_to_handle_at_EOF.extend(self._waiting_for_time_out[:count])
            self._waiting_for_time_out = self._waiting_for_time_out[count:]

        list_of_nodes = self.get_first_FCNodes()
        for node in list_of_nodes:
            """
            partial_matches = []
            for x, y in node.check_expired_timestamp.items():
                if x >= last_timestamp:
                    partial_matches.append(y)
            """
            partial_matches = [v for k, v in node.check_expired_timestamp.items() if k >= last_timestamp]
            for pm in partial_matches:
                node.check_expired_timestamp = {key: val for key, val in node.check_expired_timestamp.items() if val !=pm}
                node._left_subtree._unhandled_partial_matches.put(pm)
                node.handle_new_partial_match(node._left_subtree)

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

        # nathan - 28.06
        """
        if pm.last_timestamp in check_expired_timestamp:
            check_expired_timestamp.remove(pm.last_timestamp)
            self.check_for_expired_negative_events(pm.last_timestamp)
        """

    def get_partial_matches(self):
        """
        Returns the currently stored partial matches.
        """
        return self._partial_matches

    def get_first_FCNodes(self):

        raise NotImplementedError()

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

    def get_deepest_leave(self):

        raise NotImplementedError()

    """
    def check_for_expired_negative_events(self, last_timestamp : datetime):
        node = self.get_deepest_leave()
        # special case if the parent of the deepest leaf is a first chance node
        if type(node._parent) == FirstChanceNode:
            for pm in node.get_partial_matches():
                # call handle_new_pm ?? problem with get_last_unhandled_pm ??
                pass

        while node._parent is not None:
            if type(node._parent) == FirstChanceNode:
                node._parent._right_subtree.clean_expired_partial_matches(last_timestamp)
                for pm in node.get_partial_matches():
                    # call handle_new_pm ?? problem with get_last_unhandled_pm ??
                    # node._parent.handle_new_partial_match(pm)
                    pass
    """

class LeafNode(Node):
    """
    A leaf node is responsible for a single event type of the pattern.
    """

    def __init__(self, sliding_window: timedelta, leaf_index: int, leaf_qitem: QItem, parent: Node):
        super().__init__(sliding_window, parent)
        self.__leaf_index = leaf_index
        self.__event_name = leaf_qitem.name
        self.__event_type = leaf_qitem.event_type

        # We added an index for every QItem according to its place in the pattern in order to facilitate checking
        # if a PartialMatch is in the right order chronologically (for SEQ)
        self.qitem_index = leaf_qitem.get_event_index()

    def get_leaves(self):
        return [self]

    def get_first_FCNodes(self):
        return []

    def get_deepest_leave(self):
        return self

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

    def get_first_FCNodes(self):
        result = []
        if type(self._left_subtree) != LeafNode:
            result += self._left_subtree.get_first_FCNodes()
        if type(self._right_subtree) != LeafNode:
            result += self._right_subtree.get_first_FCNodes()
        return result

    def get_deepest_leave(self):
        if self._left_subtree is not None:
            return self._left_subtree.get_deepest_leave()

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

    def __init__(self, sliding_window: timedelta, is_first: bool, is_last: bool, parent: Node = None,
                 event_defs: List[Tuple[int, QItem]] = None,
                 left: Node = None, right: Node = None):
        super().__init__(sliding_window, parent, event_defs, left, right)
        # Those are flags in order to differenciate the NegationNode if they are in the middle of a pattern, at the beginning or at the end
        self.is_first = is_first
        self.is_last = is_last
        # This is a list of PartialMatches that are waiting to see if a later Negative event will invalidate them
        self._waiting_for_time_out = []

        # This is a list of PartialMatches that are ready
        self.matches_to_handle_at_EOF = []

    def _set_event_definitions(self,
                               left_event_defs: List[Tuple[int, QItem]], right_event_defs: List[Tuple[int, QItem]]):
        self._event_defs = merge(left_event_defs, right_event_defs, key=get_index)  # test eva à verifier

    def _merge_events_for_new_match(self,
                                    first_event_defs: List[Tuple[int, QItem]],
                                    second_event_defs: List[Tuple[int, QItem]],
                                    first_event_list: List[Event],
                                    second_event_list: List[Event]):
        return merge_according_to(first_event_defs, second_event_defs,
                                  first_event_list, second_event_list, key=lambda x: x[0])  # ici aussi faire get_index? En fait on dirait qu'on l'utilise pas finalement... A enlever?

    def get_event_definitions(self):  # to support multiple neg
        return self._left_subtree.get_event_definitions()  # à verifier

    def _try_create_new_match(self,
                              first_partial_match: PartialMatch, second_partial_match: PartialMatch,
                              first_event_defs: List[Tuple[int, QItem]], second_event_defs: List[Tuple[int, QItem]]):

        if self._sliding_window != timedelta.max and \
                abs(first_partial_match.last_timestamp - second_partial_match.first_timestamp) > self._sliding_window:
            return


        events_for_new_match = merge_according_to(first_event_defs, second_event_defs,
                                                  first_partial_match.events, second_partial_match.events, key=get_index)


        if not is_sorted(events_for_new_match,
                         key=lambda x: x.timestamp):  # 17.06 ça il faut verifier que si on est dans SEQ...
            return False

        return self._validate_new_match(events_for_new_match)

    def _validate_new_match(self, events_for_new_match: List[Event]):
        """
        Validates the condition stored in this node on the given set of events.
        """
        binding = {
            self._event_defs[i][1].name: events_for_new_match[i].payload for i in range(len(self._event_defs))
        }
        return self._condition.eval(binding)


class FirstChanceNode(InternalNegationNode):

    def __init__(self, sliding_window: timedelta, is_first: bool, is_last: bool, parent: Node = None,
                 event_defs: List[Tuple[int, QItem]] = None,
                 left: Node = None, right: Node = None):
        super().__init__(sliding_window, is_first, is_last, parent, event_defs, left, right)
        # check_expired_timestamp = []
        self.check_expired_timestamp = {}

    def handle_new_partial_match(self, partial_match_source: Node):

        if partial_match_source == self._left_subtree:
            # If we received events from the left_subtree => positive events
            # we add them to the partial matches of this node and we continue on
            new_partial_match = partial_match_source.get_last_unhandled_partial_match()  # A1 et C1
            other_subtree = self._right_subtree

            if self.is_last:
                self._waiting_for_time_out.append(new_partial_match)
                return

            first_event_defs = partial_match_source.get_event_definitions()
            other_subtree.clean_expired_partial_matches(new_partial_match.last_timestamp)

            partial_matches_to_compare = other_subtree.get_partial_matches()  # B
            second_event_defs = other_subtree.get_event_definitions()
            self.clean_expired_partial_matches(new_partial_match.last_timestamp)

            invalidate = False
            partialMatch = None
            for partialMatch in partial_matches_to_compare:
                # for every negative event, we want to check if he invalidates new_partial_match
                if self._try_create_new_match(new_partial_match, partialMatch, first_event_defs, second_event_defs):
                    invalidate = True
                    break

            # if the list is empty, there is no negative event that invalidated the current pm and therefore we go up
            if invalidate is False:
                self.add_partial_match(new_partial_match)
                if self._parent is not None:
                    self._parent.handle_new_partial_match(self)

            if invalidate:
                # if the new partial match is invalidated we want to check later if the negative event has expired
                if partialMatch.first_timestamp != partialMatch.last_timestamp:
                    print("partial match is not leaf event")
                #check_expired_timestamp.add(partialMatch.first_timestamp + self._sliding_window)
                self.check_expired_timestamp[partialMatch.last_timestamp + self._sliding_window] = new_partial_match
                # print("test:", partialMatch.last_timestamp + self._sliding_window)

            # else we do nothing ? or we need to remove the current pm from the list of pms all the way to the bottom ??
            return

        elif partial_match_source == self._right_subtree:
            # the current pm is a negative event, we check if it invalidates previous pms
            if self.is_first:
                return
            elif self.is_last:
                self.handle_PM_with_negation_at_the_end(partial_match_source)
                return
            else:
                new_partial_match = partial_match_source.get_last_unhandled_partial_match()  # A1 et C1

                other_subtree = self._left_subtree
                first_event_defs = partial_match_source.get_event_definitions()
                other_subtree.clean_expired_partial_matches(new_partial_match.last_timestamp)

                partial_matches_to_compare = other_subtree.get_partial_matches()  # B
                second_event_defs = other_subtree.get_event_definitions()
                self.clean_expired_partial_matches(new_partial_match.last_timestamp)

                partial_match_to_remove = []
                for partialMatch in partial_matches_to_compare:  # for every negative event, we want to check if he invalidates new_partial_match
                    if self._try_create_new_match(new_partial_match, partialMatch, first_event_defs, second_event_defs):
                        partial_match_to_remove.append(partialMatch)

                # if the negative event invalidated some pms we want to remove all of them in each negative node in the way up
                for partialMatch in partial_match_to_remove:
                    node = self
                    while node is not None and type(node) == FirstChanceNode:
                        node._remove_partial_matches(partialMatch)
                        node = node._parent

    def _remove_partial_matches(self, matches_to_remove: List[PartialMatch]):
        matches_to_keep = [match for match in self._partial_matches if match not in matches_to_remove]
        # for match in self._partial_matches:
        #     if match not in matches_to_remove:
        #         matches_to_keep.append(match)

        self._partial_matches = matches_to_keep
        """
        i = 0
        m = set(match_to_remove)
        while i < len(self._partial_matches):
            n = set(self._partial_matches[i].events)
            if n.issubset(set(m)):
                self._partial_matches.pop(i)
            i += 1
        self._partial_matches = [x for x in self._partial_matches.event if not set(x).issubset(m)]
        """

    # This is a customized handle_new_partial_match function especially for PartialMatch that can possibly be invalidated by a later negative event
    def handle_PM_with_negation_at_the_end(self, partial_match_source: Node):

        other_subtree = self.get_first_last_negative_node()

        new_partial_match = partial_match_source.get_last_unhandled_partial_match()  # C
        first_event_defs = partial_match_source.get_event_definitions()
        other_subtree.clean_expired_partial_matches(new_partial_match.last_timestamp)

        partial_matches_to_compare = self.get_waiting_for_time_out()
        second_event_defs = other_subtree.get_event_definitions()
        self.clean_expired_partial_matches(new_partial_match.last_timestamp)

        matches_to_keep = []
        for partialMatch in partial_matches_to_compare:  # pour chaque pm qu'on a "bloqué" on verifie si le nouveau event not va invalider
            if not self._try_create_new_match(new_partial_match, partialMatch, first_event_defs, second_event_defs):
                matches_to_keep.append(partialMatch)

        other_subtree._waiting_for_time_out = matches_to_keep

    def get_waiting_for_time_out(self):
        if (type(self._left_subtree) == PostProcessingNode or type(self._left_subtree) == FirstChanceNode) \
                and self._left_subtree.is_last:
            return self._left_subtree.get_waiting_for_time_out()
        else:
            return self._waiting_for_time_out

    # This function descends in the tree and returns us the first Node that is not a NegationNode at the end of the Pattern
    def get_first_last_negative_node(self):
        if (type(self._left_subtree) == PostProcessingNode or type(self._left_subtree) == FirstChanceNode) \
                and self._left_subtree.is_last:
            return self._left_subtree.get_first_last_negative_node()
        else:
            return self

    def get_first_FCNodes(self):
        if self.is_first:
            return [self]
        else:
            return []

class PostProcessingNode(InternalNegationNode):
    """
    An internal node connects two subtrees, i.e., two subpatterns of the evaluated pattern.
    """

    def __init__(self, sliding_window: timedelta, is_first: bool, is_last: bool, parent: Node = None,
                 event_defs: List[Tuple[int, QItem]] = None,
                 left: Node = None, right: Node = None):
        super().__init__(sliding_window, is_first, is_last, parent, event_defs, left, right)

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
        if type(self._left_subtree) == PostProcessingNode and self._left_subtree.is_last:
            return self._left_subtree.get_waiting_for_time_out()
        else:
            return self._waiting_for_time_out

    # This function descends in the tree and returns us the first Node that is not a NegationNode at the end of the Pattern
    def get_first_last_negative_node(self):
        if type(self._left_subtree) == PostProcessingNode and self._left_subtree.is_last:
            return self._left_subtree.get_first_last_negative_node()
        else:
            return self

    # This is a customized handle_new_partial_match function especially for PartialMatch that can possibly be invalidated by a later negative event
    def handle_PM_with_negation_at_the_end(self, partial_match_source: Node):

        other_subtree = self.get_first_last_negative_node()

        new_partial_match = partial_match_source.get_last_unhandled_partial_match()  # C
        first_event_defs = partial_match_source.get_event_definitions()
        other_subtree.clean_expired_partial_matches(new_partial_match.last_timestamp)

        partial_matches_to_compare = self.get_waiting_for_time_out()
        second_event_defs = other_subtree.get_event_definitions()
        self.clean_expired_partial_matches(new_partial_match.last_timestamp)

        matches_to_keep = []
        for partialMatch in partial_matches_to_compare:  # pour chaque pm qu'on a "bloqué" on verifie si le nouveau event not va invalider
            if not self._try_create_new_match(new_partial_match, partialMatch, first_event_defs, second_event_defs):
                matches_to_keep.append(partialMatch)

        other_subtree._waiting_for_time_out = matches_to_keep

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

        for partialMatch in partial_matches_to_compare:  # for every negative event, we want to check if he invalidates new_partial_match
            if self._try_create_new_match(new_partial_match, partialMatch, first_event_defs, second_event_defs):
                return

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

        # We create a tree with only the positive event and the conditions that apply to them
        temp_root = Tree.__construct_tree(pattern.structure.get_top_operator() == SeqOperator,
                                          tree_structure, pattern.structure.args, pattern.window)
        temp_root.apply_formula(pattern.condition)

        self.__root = temp_root

        # According to the flag PostProcessing or FirstChanceProcessing, we add the negative events in a different way
        PostProcessing = False
        if PostProcessing:
            self.__root = self.create_PostProcessing_Tree(temp_root, pattern)
        else:
            self.__root = self.create_FirstChanceNegation_Tree(pattern)

    def create_FirstChanceNegation_Tree(self, pattern: Pattern):
        negative_event_list = pattern.negative_event.get_args()
        origin_event_list = pattern.origin_structure.get_args()

        # nathan - 28.06
        check_expired_list =[]

        # init node to use it out of the scope of the for
        node = self.__root
        for p in negative_event_list:
            flag = 1
            p_conditions = pattern.condition.get_events_in_a_condition_with(p.get_event_name())
            set_of_depending_events = set()
            if p_conditions is not None:
                p_conditions.get_all_terms(set_of_depending_events)
            if pattern.origin_structure.get_top_operator() == SeqOperator:
                find_positive_events_before(p, set_of_depending_events, pattern.origin_structure.get_args())
            if p.get_event_name() in set_of_depending_events:
                set_of_depending_events.remove(p.get_event_name())
            # list_of_depending_events = list(set(list_of_depending_events))
            node = self.__root.get_deepest_leave()  # A CHANGER
            while flag:
                names = {item[1].name for item in node.get_event_definitions()}
                result = all(elem in names for elem in set_of_depending_events)
                counter = 0
                if result:
                    while type(node._parent) == FirstChanceNode:
                        node = node._parent
                    if p == origin_event_list[counter]:
                        temporal_root = FirstChanceNode(pattern.window, is_first=True, is_last=False)
                        counter += 1
                    elif len(negative_event_list) - negative_event_list.index(p) \
                            == len(origin_event_list) - origin_event_list.index(p):
                        temporal_root = FirstChanceNode(pattern.window, is_first=False, is_last=True)
                    else:
                        temporal_root = FirstChanceNode(pattern.window, is_first=False, is_last=False)

                    # temporal_root = InternalNegationNode(pattern.window, is_first=False, is_last=False)
                    temp_neg_event = LeafNode(pattern.window, 1, p, temporal_root)
                    temp_neg_event.apply_formula(pattern.condition)
                    temporal_root.set_subtrees(node, temp_neg_event)
                    temp_neg_event.set_parent(temporal_root)
                    temporal_root.set_parent(node._parent)
                    node.set_parent(temporal_root)
                    if temporal_root._parent != None:
                        temporal_root._parent.set_subtrees(temporal_root, temporal_root._parent._right_subtree)

                    names = {item[1].name for item in temporal_root._event_defs}
                    condition = pattern.condition.get_formula_of(names)
                    temporal_root._condition = condition if condition else TrueFormula()

                    flag = 0
                else:
                    node = node._parent

        while node._parent != None:
            node = node._parent
        self.__root = node
        # self.reorder_event_def(pattern)

        return self.__root

    def create_PostProcessing_Tree(self, temp_root: Node, pattern: Pattern):

        negative_event_list = pattern.negative_event.get_args()
        origin_event_list = pattern.origin_structure.get_args()
        counter = 0
        for p in negative_event_list:
            if p == origin_event_list[counter]:
                temporal_root = PostProcessingNode(pattern.window, is_first=True, is_last=False)
                counter += 1
            elif len(negative_event_list) - negative_event_list.index(p) \
                    == len(origin_event_list) - origin_event_list.index(p):
                temporal_root = PostProcessingNode(pattern.window, is_first=False, is_last=True)
            else:
                temporal_root = PostProcessingNode(pattern.window, is_first=False, is_last=False)

            temp_neg_event = LeafNode(pattern.window, 1, p, temporal_root)
            temporal_root.set_subtrees(temp_root, temp_neg_event)
            temp_neg_event.set_parent(temporal_root)
            temp_root.set_parent(temporal_root)
            temp_root = temp_root._parent
            # Peut être à enlever?
            names = {item[1].name for item in temp_root._event_defs}
            condition = pattern.condition.get_formula_of(names)
            temp_root._condition = condition if condition else TrueFormula()

        self.__root = temp_root
        # self.reorder_event_def(pattern)
        return self.__root
        # self.__root.apply_formula(pattern.condition)

    def get_root(self):
        return self.__root

    def reorder_event_def(self, pattern: Pattern):
        current_node = self.__root
        while type(current_node) != LeafNode:
            # if (type(current_node) == InternalNegationNode):
            current_node._event_defs.sort(key=get_index)
            current_node = current_node._left_subtree

    def handle_EOF(self, matches: Stream):
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

        # if type(self.__tree.get_root()) == InternalNegationNode and self.__tree.get_root().is_last:
        # if type(self.__tree.get_root()) == PostProcessingNode and self.__tree.get_root().is_last:
        #     self.__tree.handle_EOF(matches)
        # if type(self.__tree.get_root()) == FirstChanceNode and self.__tree.get_root().is_last:
        #     self.__tree.handle_EOF(matches)

        if (type(self.__tree.get_root()) == PostProcessingNode or type(self.__tree.get_root()) == FirstChanceNode) \
                and self.__tree.get_root().is_last:
            self.__tree.handle_EOF(matches)

        matches.close()
