from abc import ABC
from datetime import datetime
from typing import List, Set, Type
from base.Event import Event
from condition.Condition import RelopTypes, EquationSides
from base.PatternMatch import PatternMatch
from base.PatternStructure import AndOperator, SeqOperator
from misc.Utils import find_partial_match_by_timestamp, merge, \
    is_sorted, merge_according_to, calculate_joint_probability
from tree.nodes.BinaryNode import BinaryNode
from tree.nodes.Node import Node, PrimitiveEventDefinition, PatternParameters
from tree.PatternMatchStorage import TreeStorageParameters


class NegationNode(BinaryNode, ABC):
    """
    An internal node representing a negation operator.
    This implementation heavily relies on the fact that, if any unbounded negation operators are defined in the
    pattern, they are conveniently placed at the top of the tree forming a left-deep chain of nodes.
    """
    def __init__(self, pattern_params: PatternParameters, is_unbounded: bool, top_operator: Type,
                 parents: List[Node] = None, pattern_ids: int or Set[int] = None,
                 event_defs: List[PrimitiveEventDefinition] = None,
                 left: Node = None, right: Node = None):
        super().__init__(pattern_params, parents, pattern_ids, event_defs, left, right)

        # aliases for the negative node subtrees to make the code more readable
        # by construction, we always have the positive subtree on the left
        self._positive_subtree = self._left_subtree
        self._negative_subtree = self._right_subtree

        # contains the event definitions of the positive subtree
        self._positive_event_defs = None
        # negation operators that can appear in the end of the match have this flag on
        self.__is_unbounded = is_unbounded

        # the multinary operator of the root node
        self.__top_operator = top_operator

        # a list of partial matches that can be invalidated by a negative event that will only arrive in future
        self.__pending_partial_matches = []

    def set_subtrees(self, left: Node, right: Node):
        """
        Updates the aliases following the changes in the subtrees.
        """
        super().set_subtrees(left, right)
        self._positive_subtree = self._left_subtree
        self._negative_subtree = self._right_subtree

    def _set_event_definitions(self,
                               positive_event_defs: List[PrimitiveEventDefinition],
                               negative_event_defs: List[PrimitiveEventDefinition]):
        """
        Initializes the positive event definitions in addition to the full event definitions.
        """
        super()._set_event_definitions(positive_event_defs, negative_event_defs)
        self._positive_event_defs = positive_event_defs

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
            self.__pending_partial_matches = sorted(self.__pending_partial_matches, key=lambda x: x.first_timestamp)
            count = find_partial_match_by_timestamp(self.__pending_partial_matches,
                                                    last_timestamp - self._sliding_window)
            matches_to_flush = self.__pending_partial_matches[:count]
            self.__pending_partial_matches = self.__pending_partial_matches[count:]
        else:
            matches_to_flush = self.__pending_partial_matches

        # since matches_to_flush could be expired, we need to temporarily disable timestamp checks
        Node._toggle_enable_partial_match_expiration(False)
        for partial_match in matches_to_flush:
            super()._add_partial_match(partial_match)
        Node._toggle_enable_partial_match_expiration(True)

    def get_positive_event_definitions(self):
        """
        Returns the positive event definitions (as opposed to self._event_defs returned by get_event_definitions and
        also containing the definitions of the negative events).
        """
        return self._positive_event_defs

    def _try_create_new_matches(self, new_partial_match: PatternMatch, partial_matches_to_compare: List[PatternMatch],
                                first_event_defs: List[PrimitiveEventDefinition],
                                second_event_defs: List[PrimitiveEventDefinition]):
        """
        The flow of this method is the opposite of the one its superclass implements. For each pair of a positive and a
        negative partial match, we combine the two sides to form a new partial match, validate it, and then do nothing
        if the validation succeeds (i.e., the negative part invalidated the positive one), and transfer the positive
        match up the tree if the validation fails.
        """
        positive_events = new_partial_match.events
        probability = new_partial_match.probability
        for partial_match in partial_matches_to_compare:
            negative_events = partial_match.events
            combined_event_list = self._merge_events_for_new_match(first_event_defs, second_event_defs,
                                                                   positive_events, negative_events)
            if not self._validate_new_match(combined_event_list):
                continue
            # a negative partial match affecting the potential positive partial match was found
            if partial_match.probability is not None:
                # an uncertain negative partial match - the probability of the new match should be reduced
                probability = calculate_joint_probability(probability, 1 - partial_match.probability)
            else:
                # this match should not be transferred
                # TODO: the rejected positive partial match should be explicitly removed to save space
                return
        # no negative match invalidated the positive one - we can go on
        self._propagate_partial_match(positive_events, probability)

    def _add_partial_match(self, pm: PatternMatch):
        """
        If this node can receive unbounded negative events and is the deepest node in the tree to do so, a
        successfully evaluated partial match must be added to a dedicated waiting list rather than propagated normally.
        """
        if self.__is_first_unbounded_negative_node():
            self.__pending_partial_matches.append(pm)
        else:
            super()._add_partial_match(pm)

    def handle_new_partial_match(self, partial_match_source: Node):
        """
        For positive partial matches, activates the flow of the superclass. For negative partial matches, does nothing
        for bounded events (as nothing should be done in this case), otherwise checks whether existing positive matches
        must be invalidated and handles them accordingly.
        """
        if partial_match_source == self._positive_subtree:
            # a new positive partial match has arrived
            super().handle_new_partial_match(partial_match_source)
            return
        # a new negative partial match has arrived
        if not self.__is_unbounded:
            # no unbounded negatives - there is nothing to do
            return

        # this partial match contains unbounded negative events
        first_unbounded_node = self.get_first_unbounded_negative_node()
        positive_event_defs = first_unbounded_node.get_positive_event_definitions()

        unbounded_negative_partial_match = partial_match_source.get_last_unhandled_partial_match_by_parent(self)
        negative_event_defs = partial_match_source.get_event_definitions_by_parent(self)

        matches_to_keep = []
        for positive_partial_match in first_unbounded_node.__pending_partial_matches:
            combined_event_list = self._merge_events_for_new_match(positive_event_defs,
                                                                   negative_event_defs,
                                                                   positive_partial_match.events,
                                                                   unbounded_negative_partial_match.events)
            if not self._validate_new_match(combined_event_list):
                # this positive match should still be kept
                matches_to_keep.append(positive_partial_match)

        first_unbounded_node.__pending_partial_matches = matches_to_keep

    def get_first_unbounded_negative_node(self):
        """
        Returns the deepest unbounded node in the tree. This node keeps the partial matches that are pending release
        due to the presence of unbounded negative events in the pattern.
        """
        if not self.__is_unbounded:
            return None
        return self if self.__is_first_unbounded_negative_node() \
            else self._positive_subtree.get_first_unbounded_negative_node()

    def __is_first_unbounded_negative_node(self):
        """
        Returns True if this node is the first unbounded negative node and False otherwise.
        """
        return self.__is_unbounded and \
            (not isinstance(self._positive_subtree, NegationNode) or not self._positive_subtree.__is_unbounded)

    def create_storage_unit(self, storage_params: TreeStorageParameters, sorting_key: callable = None,
                            rel_op: RelopTypes = None, equation_side: EquationSides = None,
                            sort_by_first_timestamp: bool = False):
        """
        For now, only the most trivial storage settings will be supported by negative nodes.
        """
        self._init_storage_unit(storage_params, sorting_key, rel_op, equation_side)
        self._positive_subtree.create_storage_unit(storage_params)
        self._negative_subtree.create_storage_unit(storage_params)

    def is_equivalent(self, other):
        """
        In addition to the checks performed by the base class, separately verifies the equivalence of the positive and
        the negative subtrees.
        """
        if not super().is_equivalent(other):
            return False
        v1 = self._positive_subtree.is_equivalent(other.get_left_subtree())
        v2 = self._negative_subtree.is_equivalent(other.get_right_subtree())
        return v1 and v2


class NegativeAndNode(NegationNode):
    """
    An internal node representing a negative conjunction operator.
    """
    def __init__(self, pattern_params: PatternParameters, is_unbounded: bool,
                 parents: List[Node] = None, pattern_ids: int or Set[int] = None,
                 event_defs: List[PrimitiveEventDefinition] = None,
                 left: Node = None, right: Node = None):
        super().__init__(pattern_params, is_unbounded, AndOperator, parents, pattern_ids, event_defs, left, right)

    def get_structure_summary(self):
        return ("NAnd",
                self._positive_subtree.get_structure_summary(),
                self._negative_subtree.get_structure_summary())


class NegativeSeqNode(NegationNode):
    """
    An internal node representing a negative sequence operator.
    Unfortunately, this class contains some code duplication from SeqNode to avoid diamond inheritance.
    """
    def __init__(self, pattern_params: PatternParameters, is_unbounded: bool,
                 parents: List[Node] = None, pattern_ids: int or Set[int] = None,
                 event_defs: List[PrimitiveEventDefinition] = None,
                 left: Node = None, right: Node = None):
        super().__init__(pattern_params, is_unbounded, SeqOperator, parents, pattern_ids, event_defs, left, right)

    def get_structure_summary(self):
        return ("NSeq",
                self._positive_subtree.get_structure_summary(),
                self._negative_subtree.get_structure_summary())

    def _set_event_definitions(self,
                               positive_event_defs: List[PrimitiveEventDefinition],
                               negative_event_defs: List[PrimitiveEventDefinition]):
        self._event_defs = merge(positive_event_defs, negative_event_defs, key=lambda x: x.index)
        self._positive_event_defs = positive_event_defs

    def _validate_new_match(self, events_for_new_match: List[Event]):
        if not is_sorted(events_for_new_match, key=lambda x: x.timestamp):
            return False
        return super()._validate_new_match(events_for_new_match)

    def _merge_events_for_new_match(self,
                                    first_event_defs: List[PrimitiveEventDefinition],
                                    second_event_defs: List[PrimitiveEventDefinition],
                                    first_event_list: List[Event],
                                    second_event_list: List[Event]):
        return merge_according_to(first_event_defs, second_event_defs,
                                  first_event_list, second_event_list, key=lambda x: x.index)
