from evaluation.Nodes.Node import Node
from typing import List, Tuple
from datetime import timedelta, datetime
from base.Event import Event
from base.Formula import Formula, AtomicFormula, TrueFormula
from evaluation.PartialMatch import PartialMatch
from base.PatternStructure import SeqOperator, QItem

# from base.Event import Event TODO

from misc.Utils import (
    merge,
    merge_according_to,
    is_sorted,
    find_partial_match_by_timestamp,
)
from evaluation.Storage import SortedStorage, UnsortedStorage
import json
from evaluation.Nodes.LeafNode import LeafNode


class InternalNode(Node):
    """
    An internal node connects two subtrees, i.e., two subpatterns of the evaluated pattern.
    """

    def __init__(
        self,
        sliding_window: timedelta,
        parent: Node = None,
        event_defs: List[Tuple[int, QItem]] = None,
        left: Node = None,
        right: Node = None,
    ):
        super().__init__(sliding_window, parent)
        self._event_defs = event_defs
        self._left_subtree = left
        self._right_subtree = right

        """MY NEW FIELDS"""
        self._relation_op = None  # just for json_repr
        self._simplified_condition = (None, None, None)  # just for json_repr
        """MY NEW FIELDS"""

    def get_leaves(self):
        result = []
        if self._left_subtree is not None:
            result += self._left_subtree.get_leaves()
        if self._right_subtree is not None:
            result += self._right_subtree.get_leaves()
        return result

    def apply_formula(self, formula: Formula):
        if formula is None:
            return
        names = {item[1].name for item in self._event_defs}
        condition = formula.get_formula_of(names)
        self._condition = condition if condition else TrueFormula()
        self._left_subtree.apply_formula(self._condition)
        self._right_subtree.apply_formula(self._condition)

    def get_event_definitions(self):
        return self._event_defs

    def _set_event_definitions(
        self, left_event_defs: List[Tuple[int, QItem]], right_event_defs: List[Tuple[int, QItem]],
    ):
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
        self._set_event_definitions(
            self._left_subtree.get_event_definitions(), self._right_subtree.get_event_definitions(),
        )
        # maybe add these here:  set sorting properties
        # apply formula according to left and right and simplify
        # and apply sequence formula

    def handle_new_partial_match(self, partial_match_source: Node):
        """
        Internal node's update for a new partial match in one of the subtrees.
        """
        if partial_match_source == self._left_subtree:
            other_subtree = self._right_subtree
            # curr_term = self._condition.left_term  # NADER
            # side_of_equation = "right"  # NADER
        elif partial_match_source == self._right_subtree:
            other_subtree = self._left_subtree
            # curr_term = self._condition.right_term  # NADER
            # side_of_equation = "left"  # NADER
        else:
            raise Exception()  # should never happen
        # gets only the new one
        new_partial_match = partial_match_source.get_last_unhandled_partial_match()
        new_pm_key = partial_match_source._partial_matches.get_key()
        first_event_defs = partial_match_source.get_event_definitions()
        # cleaning other subtree before receiving anything from it
        other_subtree.clean_expired_partial_matches(new_partial_match.last_timestamp)
        # from other_subtree we need to get a compact amount of partial matches

        partial_matches_to_compare = other_subtree.get_partial_matches(new_pm_key(new_partial_match))
        # print("new_pm = " + repr(new_partial_match))
        # print("tocompare_pms = " + repr(partial_matches_to_compare))
        second_event_defs = other_subtree.get_event_definitions()

        # child is cleaned when his father handles his partial matches,
        # this is only necessary for root, possibly redundant code.(maybe check if root)
        self.clean_expired_partial_matches(new_partial_match.last_timestamp)

        # given a partial match from one subtree, for each partial match
        # in the other subtree we check for new partial matches in this node.

        # here ATM we iterate over all the possible partial matches,however most of them aren't relevant
        for partialMatch in partial_matches_to_compare:
            self._try_create_new_match(new_partial_match, partialMatch, first_event_defs, second_event_defs)

    def _try_create_new_match(
        self,
        first_partial_match: PartialMatch,
        second_partial_match: PartialMatch,
        first_event_defs: List[Tuple[int, QItem]],
        second_event_defs: List[Tuple[int, QItem]],
    ):
        """
        Verifies all the conditions for creating a new partial match and creates it if all constraints are satisfied.
        """
        if (
            self._sliding_window != timedelta.max
            and abs(first_partial_match.last_timestamp - second_partial_match.first_timestamp) > self._sliding_window
        ):
            return

        events_for_new_match = self._merge_events_for_new_match(
            first_event_defs, second_event_defs, first_partial_match.events, second_partial_match.events,
        )
        # events merged
        if not self._validate_new_match(events_for_new_match):
            return
        self.add_partial_match(PartialMatch(events_for_new_match))
        if self._parent is not None:
            self._parent.handle_new_partial_match(self)

    def _merge_events_for_new_match(
        self,
        first_event_defs: List[Tuple[int, QItem]],
        second_event_defs: List[Tuple[int, QItem]],
        first_event_list: List[Event],
        second_event_list: List[Event],
    ):
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
        binding = {self._event_defs[i][1].name: events_for_new_match[i].payload for i in range(len(self._event_defs))}
        return self._condition.eval(binding)

    def add_partial_match(self, pm: PartialMatch):
        """
        Registers a new partial match at this node.
        As of now, the insertion is always by the timestamp, and the partial matches are stored in a list sorted by
        timestamp. Therefore, the insertion operation is performed in O(log n).
        """
        # adds new partial match to list O(logn)
        # adds new partial match queue for continuing the computation upwards
        self._partial_matches.add(pm)  # MUH
        """OLD
        index = find_partial_match_by_timestamp(
            self._partial_matches, pm.first_timestamp
        )
        self._partial_matches.insert(index, pm)
        OLD"""
        if self._parent is not None:
            self._unhandled_partial_matches.put(pm)

    def json_repr(self):
        events_defs_for_json = map(lambda x: x[0], self._event_defs)
        events_nums = list(events_defs_for_json)
        node_type = "SEQ Node"
        if type(self) == AndNode:
            node_type = "AND Node"
        data_set = {
            "Node": node_type,
            "event defs": events_nums,
            "not_simplified_condition": repr(self._condition),
            "simplified_condition": "{} {} {}".format(
                self._simplified_condition[0], self._simplified_condition[1], self._simplified_condition[2]
            ),
            "RELOP": self._relation_op,
            "": "",
            "left_subtree": self._left_subtree.json_repr(),
            "right_subtree": self._right_subtree.json_repr(),
            "pms": repr(self._partial_matches),
        }
        return data_set


"""
def extract_from_formula(simple_formula: Formula):
    if isinstance(simple_formula, AtomicFormula):
        return (
            simple_formula.left_term,
            simple_formula.relation_op,
            simple_formula.right_term,
        )
    else:
        return (None, None, None)
"""


class AndNode(InternalNode):
    """
    An internal node representing an "AND" operator.
    """

    # TODO: add a constructor that call super_init() fro internal and construct the storage according to AND or seq NODE TODO

    # we should agree with SeqNode on a mutual definition if this function
    # creates aa Storage unit with the key chosen by it's father and chooses the sorting_key for its children
    # if you are calling this function on root then sorting_key can be WHATEVER you want
    def create_storage_unit(self, sorting_key: callable, relation_op=None, equation_side=None):
        """if ENABLE_SORTING == false:
            self._partial_matches = UnsortedStorage([])
            self._left_subtree.create_storage_unit(None)
            self._right_subtree.create_storage_unit(None)
            return"""

        if sorting_key is None:
            self._partial_matches = UnsortedStorage([])
        else:
            self._partial_matches = SortedStorage(sorting_key, relation_op, equation_side)

        left_sorting_key = None
        right_sorting_key = None
        relop = None
        # ////////////////////////////////////////////////////////////////
        left_event_defs = self._left_subtree.get_event_definitions()
        right_event_defs = self._right_subtree.get_event_definitions()
        left_event_names = {item[1].name for item in left_event_defs}
        right_event_names = {item[1].name for item in right_event_defs}

        left_term, relop, right_term = self._condition.simplify_formula(left_event_names, right_event_names)

        if relop is not None:
            # left_term, relop, right_term = extract_from_formula(simple_formula)
            self._relation_op = relop
            self._simplified_condition = (left_term, relop, right_term)
            left_sorting_key = lambda pm: left_term.eval(
                {left_event_defs[i][1].name: pm.events[i].payload for i in range(len(pm.events))}
            )
            right_sorting_key = lambda pm: right_term.eval(
                {right_event_defs[i][1].name: pm.events[i].payload for i in range(len(pm.events))}
            )
        # ////////////////////////////////////////////////////////////////
        # both sons not sorted by first_timestamp
        self._left_subtree.create_storage_unit(left_sorting_key, relop, "left")
        self._right_subtree.create_storage_unit(right_sorting_key, relop, "right")


class SeqNode(InternalNode):
    """
    An internal node representing a "SEQ" (sequence) operator.
    In addition to checking the time window and condition like the basic node does, SeqNode also verifies the order
    of arrival of the events in the partial matches it constructs.
    """

    def create_storage_unit(self, sorting_key: callable, relation_op=None, equation_side=None):
        """
        This function creates the storage for partial_matches it gives a special key: callable
        to the storage unit which tells the storage unit on which attribute(only timestamps here)
        to sort.
        We assume all events are in SEQ(,,,,...) which makes the order in partial match the same
        as in event_defs: [(1,a),(2,b)] in event_defs and [a,b] in pm.
        """
        if sorting_key is None:
            self._partial_matches = UnsortedStorage([])
        else:
            self._partial_matches = SortedStorage([], sorting_key, relation_op, equation_side)

        left_event_defs = self._left_subtree.get_event_definitions()
        right_event_defs = self._right_subtree.get_event_definitions()
        # comparing min and max leaf index of two subtrees
        min_left = min(left_event_defs, key=lambda x: x[0])[0]  # [ { ] } or [ { } ]
        max_left = max(left_event_defs, key=lambda x: x[0])[0]  # { [ } ] or { [ ] }
        min_right = min(right_event_defs, key=lambda x: x[0])[0]  # [ ] { }
        max_right = max(right_event_defs, key=lambda x: x[0])[0]  # { } [ ]
        # print("min_left: {}".format(min_left))
        # print("max_left: {}".format(max_left))
        # print("min_right: {}".format(min_right))
        # print("max_right: {}".format(max_right))
        # left_leaf_index = left_event_defs[0][0]
        # right_leaf_index = right_event_defs[0][0]
        left_sort = 0
        right_sort = 0
        relop = None
        if max_left < min_right:  # 3)
            left_sort = -1
            right_sort = 0
            # left_leaf_index = left_event_defs[-1][0]
            relop = "<"

        elif max_right < min_left:  # 4)
            left_sort = 0
            right_sort = -1
            # right_leaf_index = right_event_defs[-1][0]
            relop = ">"

        elif min_left < min_right:  # 1)
            relop = "<"

        elif min_right < min_left:  # 2)
            relop = ">"

        assert relop is not None

        self._relation_op = relop  # just for the json_repr
        self._left_subtree.create_storage_unit(lambda pm: pm.events[left_sort].timestamp, relop, "left")
        self._right_subtree.create_storage_unit(lambda pm: pm.events[right_sort].timestamp, relop, "right")

    def _set_event_definitions(
        self, left_event_defs: List[Tuple[int, QItem]], right_event_defs: List[Tuple[int, QItem]],
    ):
        self._event_defs = merge(left_event_defs, right_event_defs, key=lambda x: x[0])

    def _merge_events_for_new_match(
        self,
        first_event_defs: List[Tuple[int, QItem]],
        second_event_defs: List[Tuple[int, QItem]],
        first_event_list: List[Event],
        second_event_list: List[Event],
    ):
        return merge_according_to(
            first_event_defs, second_event_defs, first_event_list, second_event_list, key=lambda x: x[0],
        )

    def _validate_new_match(self, events_for_new_match: List[Event]):
        if not is_sorted(events_for_new_match, key=lambda x: x.timestamp):  # validates timestamps
            return False
        return super()._validate_new_match(events_for_new_match)  # validates conditons
