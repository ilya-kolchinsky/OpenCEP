from Node import Node
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

        # child is cleaned when his father handles his partial matches,
        # this is only necessary for root, possibly redundant code.(maybe check if root)
        self.clean_expired_partial_matches(new_partial_match.last_timestamp)

        # given a partial match from one subtree, for each partial match
        # in the other subtree we check for new partial matches in this node.

        # here ATM we iterate over all the possible partial matches,however most of them aren't relevant
        # we want a new mechanism to improve the time complexity to atleast O(log(n))
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
        if not is_sorted(events_for_new_match, key=lambda x: x.timestamp):
            return False
        return super()._validate_new_match(events_for_new_match)
