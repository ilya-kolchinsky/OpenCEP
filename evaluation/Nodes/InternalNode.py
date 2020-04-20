from evaluation.Nodes.Node import Node
from typing import List, Tuple
from datetime import timedelta, datetime
from base.Formula import *
from evaluation.PartialMatch import PartialMatch
from base.PatternStructure import SeqOperator, QItem

# from base.Event import Event TODO
from evaluation.temp_simple_modules import *

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
        self._relational_op = None  # can be '==' '!=' '<' '<=' '>' '>=' and None if not order is  enforced if any of the sons is unsortable
        self._smaller_side = None  # TODO to remove this
        self._greater_side = None  # TODO to remove this
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
        self,
        left_event_defs: List[Tuple[int, QItem]],
        right_event_defs: List[Tuple[int, QItem]],
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
            self._left_subtree.get_event_definitions(),
            self._right_subtree.get_event_definitions(),
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
        elif partial_match_source == self._right_subtree:
            other_subtree = self._left_subtree
        else:
            raise Exception()  # should never happen
        # gets only the new one
        new_partial_match = partial_match_source.get_last_unhandled_partial_match()
        new_pm_key = partial_match_source._partial_matches.get_sorting_key()
        first_event_defs = partial_match_source.get_event_definitions()
        # cleaning other subtree before receiving anything from it
        other_subtree.clean_expired_partial_matches(new_partial_match.last_timestamp)
        # from other_subtree we need to get a compact amount of partial matches
        if other_subtree == self._greater_side:
            partial_matches_to_compare = other_subtree.get_partial_matches(
                new_partial_match, new_pm_key, Greater=True
            )
            print("WTTTTFF1111")
        elif other_subtree == self._smaller_side:
            partial_matches_to_compare = other_subtree.get_partial_matches(
                new_partial_match, new_pm_key, Greater=False
            )
            print("WTTTTFF222222")
        print("new_pm = " + repr(new_partial_match))
        print("tocompare_pms = " + repr(partial_matches_to_compare))
        second_event_defs = other_subtree.get_event_definitions()

        # child is cleaned when his father handles his partial matches,
        # this is only necessary for root, possibly redundant code.(maybe check if root)
        self.clean_expired_partial_matches(new_partial_match.last_timestamp)

        # given a partial match from one subtree, for each partial match
        # in the other subtree we check for new partial matches in this node.

        # here ATM we iterate over all the possible partial matches,however most of them aren't relevant
        for partialMatch in partial_matches_to_compare:
            self._try_create_new_match(
                new_partial_match, partialMatch, first_event_defs, second_event_defs
            )

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
            and abs(
                first_partial_match.last_timestamp
                - second_partial_match.first_timestamp
            )
            > self._sliding_window
        ):
            return

        events_for_new_match = self._merge_events_for_new_match(
            first_event_defs,
            second_event_defs,
            first_partial_match.events,
            second_partial_match.events,
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
        binding = {
            self._event_defs[i][1].name: events_for_new_match[i].payload
            for i in range(len(self._event_defs))
        }
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


"""
def extract_from_formula(simple_formula: Formula):
    if isinstance(simple_formula, AtomicFormula):
        if type(simple_formula)
    (simple_condition.left_term, simple_condition, simple_condition.right_term)
"""


class AndNode(InternalNode):
    """
    An internal node representing an "AND" operator.
    """

    # we should agree with SeqNode on a mutual definition if this function
    # creates aa Storage unit with the key chosen by it's father and chooses the sorting_key for its children
    # if you are calling this function on root then sorting_key can be WHATEVER you want
    def create_storage_unit(
        self, sorting_key: callable, is_sorted_by_first_timestamp=False
    ):

        if sorting_key is None:
            self._partial_matches = UnsortedStorage([])
        else:
            self._partial_matches = SortedStorage(
                [], sorting_key, is_sorted_by_first_timestamp
            )

        left_event_defs = self._left_subtree.get_event_definitions()
        right_event_defs = self._right_subtree.get_event_definitions()

        simple_formula = simplified_formula(
            self._condition, left_event_defs, right_event_defs
        )  # left and right_sorting _keys are ACCORDING TO left and right_event _defs
        if simple_formula is not None:
            lhs, self._relational_op, rhs = extract_from_formula(simple_formula)
            left_sorting_key = lambda pm: lhs.eval(
                {
                    left_event_defs[i][1]: pm.events[i].payload
                    for i in range(len(pm.events))
                }
            )
            right_sorting_key = lambda pm: rhs.eval(
                {
                    right_event_defs[i][1]: pm.events[i].payload
                    for i in range(len(pm.events))
                }
            )

        # both sons not sorted by first_timestamp
        self._left_subtree.create_storage_unit(left_sorting_key)
        self._right_subtree.create_storage_unit(right_sorting_key)


class SeqNode(InternalNode):
    """
    An internal node representing a "SEQ" (sequence) operator.
    In addition to checking the time window and condition like the basic node does, SeqNode also verifies the order
    of arrival of the events in the partial matches it constructs.
    """

    def create_storage_unit(
        self, sorting_key: callable, is_sorted_by_first_timestamp=False
    ):
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
            self._partial_matches = SortedStorage(
                [], sorting_key, is_sorted_by_first_timestamp
            )

        left_event_defs = self._left_subtree.get_event_definitions()
        right_event_defs = self._right_subtree.get_event_definitions()
        # comparing min and max leaf index of two subtrees
        min_left = min(left_event_defs, key=lambda x: x[0])[0]
        max_left = max(left_event_defs, key=lambda x: x[0])[0]
        min_right = min(right_event_defs, key=lambda x: x[0])[0]
        max_right = max(right_event_defs, key=lambda x: x[0])[0]
        """ [] is min_left and max_left, {} is min_right and max_right
        1) [ { ] }    [ { } ]      minleft , minright ,  smaller_side = left
        2) { [ } ]    { [ ] }      minright , minleft , smaller_side = right
        3) [ ] { }       maxleft minright smaller_side = left
        4) { } [ ]       maxright minleft smaller_side = right
        """
        left_leaf_index = left_event_defs[0][0]
        right_leaf_index = right_event_defs[0][0]
        if max_left < min_right:  # 3)
            left_leaf_index = left_event_defs[-1][0]
            self._relational_op = "<"
        elif max_right < min_left:  # 4)
            right_leaf_index = right_event_defs[-1][0]
            self._relational_op = ">"
        elif min_left < min_right:  # 1)
            self._relational_op = "<"
        elif min_right < min_left:  # 2)
            self._relational_op = ">"
        # TODO make sure the next two lines qork or false or true
        left_is_sorted_by_first_timestamp = left_leaf_index == left_event_defs[0][0]
        right_is_sorted_by_first_timestamp = right_leaf_index == right_event_defs[0][0]
        self._left_subtree.create_storage_unit(
            lambda pm: pm.events[left_leaf_index].timestamp,
            left_is_sorted_by_first_timestamp,
        )
        self._right_subtree.create_storage_unit(
            lambda pm: pm.events[right_leaf_index].timestamp,
            right_is_sorted_by_first_timestamp,
        )

    def _set_event_definitions(
        self,
        left_event_defs: List[Tuple[int, QItem]],
        right_event_defs: List[Tuple[int, QItem]],
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
            first_event_defs,
            second_event_defs,
            first_event_list,
            second_event_list,
            key=lambda x: x[0],
        )

    def _validate_new_match(self, events_for_new_match: List[Event]):
        if not is_sorted(
            events_for_new_match, key=lambda x: x.timestamp
        ):  # validates timestamps
            return False
        return super()._validate_new_match(events_for_new_match)  # validates conditons

    def json_repr(self):
        events_defs_for_json = map(lambda x: x[0], self._event_defs)
        events_nums = list(events_defs_for_json)
        smaller_side_dict = self._smaller_side.json_repr()
        greater_side_dict = self._greater_side.json_repr()
        if type(self._smaller_side) == LeafNode:
            sm_sd_event_defs_key = "leaf index"
        else:
            sm_sd_event_defs_key = "event defs"

        if type(self._greater_side) == LeafNode:
            gr_sd_event_defs_key = "leaf index"
        else:
            gr_sd_event_defs_key = "event defs"
        data_set = {
            "Node": "SeqNode",
            "event defs": events_nums,
            "condition": repr(self._condition),
            "": "",
            "smaller side": {
                "Node": smaller_side_dict["Node"],
                sm_sd_event_defs_key: smaller_side_dict[sm_sd_event_defs_key],
            },
            "greater side": {
                "Node": greater_side_dict["Node"],
                gr_sd_event_defs_key: greater_side_dict[gr_sd_event_defs_key],
            },
            "left_subtree": self._left_subtree.json_repr(),
            "right_subtree": self._right_subtree.json_repr(),
            "pms": repr(self._partial_matches),
        }
        return data_set
