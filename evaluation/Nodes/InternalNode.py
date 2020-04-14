from evaluation.Nodes.Node import Node
from typing import List, Tuple
from datetime import timedelta, datetime
from base.Formula import TrueFormula, Formula
from evaluation.PartialMatch import PartialMatch
from base.PatternStructure import SeqOperator, QItem
from base.Event import Event
from misc.Utils import (
    merge,
    merge_according_to,
    is_sorted,
    find_partial_match_by_timestamp,
)
from Storage import ArrayStorage


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

    def get_partial_matches(self, new_partial_match: PartialMatch):
        """
        Returns the currently stored partial matches.
        """
        pass
        # return self._partial_matches.get(new_partial_match)

    def get_leaves(self):
        result = []
        if self._left_subtree is not None:
            result += self._left_subtree.get_leaves()
        if self._right_subtree is not None:
            result += self._right_subtree.get_leaves()
        return result

    def apply_formula(
        self, formula: Formula
    ):  # this function is recursive now I want it non-recursive and applied when setting sub trees
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
        # set sorting properties
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

        new_partial_match = (
            partial_match_source.get_last_unhandled_partial_match()
        )  # gets only the new one
        first_event_defs = partial_match_source.get_event_definitions()

        other_subtree.clean_expired_partial_matches(new_partial_match.last_timestamp)

        # this is the function that needs ENHANCEMENT
        partial_matches_to_compare = other_subtree.get_partial_matches(
            new_partial_match, Greater=True
        )
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

    def create_storage_unit(self, leaf_index: int):
        # if node is root which key?
        if leaf_index == -1:
            self._partial_matches = ArrayStorage()
        # assuming all events are in SEQ(,,,,...) makes the order in partial match the same as in event_defs [(1,a),(2,b)] in event_defs and [a,b] in pm
        else:
            self._partial_matches = ArrayStorage(
                array=[], key=lambda pm: pm.events[leaf_index].timestamp
            )

        left_event_defs = self._left_subtree.get_event_definitions()
        right_event_defs = self._right_subtree.get_event_definitions()
        # comparing min and max leaf index of two subtrees
        min_left = min(left_event_defs, key=lambda x: x[0])[0]
        max_left = max(left_event_defs, key=lambda x: x[0])[0]
        min_right = min(right_event_defs, key=lambda x: x[0])[0]
        max_right = max(right_event_defs, key=lambda x: x[0])[0]
        # if the other ifs don't apply then we take the first_timestamp
        left_leaf_index = left_event_defs[0][0]
        right_leaf_index = right_event_defs[0][0]
        # TODO self._left_is_smaller = False
        # {left} [right]        if all left subtree should be before all of right subtree (1,2) , (3,4)
        if max_left < min_right:
            left_leaf_index = left_event_defs[-1][0]

        # TODO if min_left < min_right:
        # self._left_is_smaller = True
        # [right] {left}         if all right subtree should be before all of left subtree (3,4) , (1,2)
        elif max_right < min_left:
            right_leaf_index = right_event_defs[-1][0]

        self._left_subtree.create_storage_unit(left_leaf_index)
        self._right_subtree.create_storage_unit(right_leaf_index)

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
