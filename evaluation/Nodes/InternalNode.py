from evaluation.Nodes.Node import Node
from typing import List, Tuple
from datetime import timedelta, datetime
from base.Event import Event
from base.Formula import Formula, AtomicFormula, TrueFormula, RelopTypes, EquationSides
from evaluation.PartialMatch import PartialMatch
from base.PatternStructure import SeqOperator, QItem
from misc.Utils import merge, merge_according_to, is_sorted
from evaluation.Storage import SortedStorage, UnsortedStorage, TreeStorageParameters


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
        new_pm_key = partial_match_source._partial_matches.get_key()
        first_event_defs = partial_match_source.get_event_definitions()
        other_subtree.clean_expired_partial_matches(new_partial_match.last_timestamp)
        partial_matches_to_compare = other_subtree.get_partial_matches(new_pm_key(new_partial_match))
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
        # We need this because clean_expired doesn't necessarily clean old partial matches.
        if self._sliding_window != timedelta.max and (
            abs(first_partial_match.last_timestamp - second_partial_match.first_timestamp) > self._sliding_window
            or abs(first_partial_match.first_timestamp - second_partial_match.last_timestamp) > self._sliding_window
        ):
            return
        events_for_new_match = self._merge_events_for_new_match(first_event_defs, second_event_defs,
                                                                first_partial_match.events, second_partial_match.events)
        # events merged
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

    def _get_condition_based_sorting_keys(self, attributes_priorities):
        left_sorting_key, right_sorting_key, relop = None, None, None
        
        left_event_defs = self._left_subtree.get_event_definitions()
        right_event_defs = self._right_subtree.get_event_definitions()
        left_event_names = {item[1].name for item in left_event_defs}
        right_event_names = {item[1].name for item in right_event_defs}
        simple_formula = self._condition.simplify_formula(left_event_names, right_event_names,attributes_priorities)
        if simple_formula is not None:
            left_term, relop, right_term = simple_formula.dismantle()
            left_sorting_key = lambda pm: left_term.eval(
                {left_event_defs[i][1].name: pm.events[i].payload for i in range(len(pm.events))}
            )
            right_sorting_key = lambda pm: right_term.eval(
                {right_event_defs[i][1].name: pm.events[i].payload for i in range(len(pm.events))}
            )

        return left_sorting_key, right_sorting_key, relop

    def _get_sequence_based_sorting_keys(self):
        left_event_defs = self._left_subtree.get_event_definitions()
        right_event_defs = self._right_subtree.get_event_definitions()
        # comparing min and max leaf index of two subtrees
        min_left = min(left_event_defs, key=lambda x: x[0])[0]  # [ { ] } or [ { } ]
        max_left = max(left_event_defs, key=lambda x: x[0])[0]  # { [ } ] or { [ ] }
        min_right = min(right_event_defs, key=lambda x: x[0])[0]  # [ ] { }
        max_right = max(right_event_defs, key=lambda x: x[0])[0]  # { } [ ]

        if max_left < min_right:  # 3)
            left_sort, right_sort, relop = -1, 0, RelopTypes.SmallerEqual
        elif max_right < min_left:  # 4)
            left_sort, right_sort, relop = 0, -1, RelopTypes.GreaterEqual
        elif min_left < min_right:  # 1)
            left_sort, right_sort, relop = 0, 0, RelopTypes.SmallerEqual
        elif min_right < min_left:  # 2)
            left_sort, right_sort, relop = 0, 0, RelopTypes.GreaterEqual
        assert relop is not None
        left_sorting_key = lambda pm: pm.events[left_sort].timestamp
        right_sorting_key = lambda pm: pm.events[right_sort].timestamp
        # left/right_sort == 0 means that left/right subtree will be sorted by first timestamp
        return left_sorting_key, right_sorting_key, relop, (left_sort == 0), (right_sort == 0)

    def _init_storage_unit(self, sort_storage, sorting_key, relation_op, equation_side, clean_expired_every,
                           sort_by_first_timestamp=False):
        if not sort_storage or sorting_key is None:
            self._partial_matches = UnsortedStorage(clean_expired_every, sorting_key)
        else:
            self._partial_matches = SortedStorage(sorting_key, relation_op, equation_side,
                                                  clean_expired_every, sort_by_first_timestamp)

class AndNode(InternalNode):
    """
    An internal node representing an "AND" operator.
    """
    def create_storage_unit(self, storage_params: TreeStorageParameters, sorting_key: callable=None,
                            relation_op=None, equation_side=None, sort_by_first_timestamp=False):
        self._init_storage_unit(storage_params.sort_storage, sorting_key, relation_op, equation_side,
                                storage_params.clean_expired_every)
        left_key, right_key, relop = None, None, None                                
        if storage_params.sort_storage:
            left_key, right_key, relop = self._get_condition_based_sorting_keys(storage_params.attributes_priorities)
        self._left_subtree.create_storage_unit(storage_params, left_key, relop, EquationSides.left)
        self._right_subtree.create_storage_unit(storage_params, right_key, relop, EquationSides.right)


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

    def create_storage_unit(self, storage_params: TreeStorageParameters, sorting_key: callable = None,
                            relation_op=None, equation_side=None, sort_by_first_timestamp=False):
        """
        This function creates the storage for partial_matches it gives a special key: callable
        to the storage unit which tells the storage unit on which attribute(only timestamps here)
        to sort.
        We assume all events are in SEQ(,,,,...) which makes the order in partial match the same
        as in event_defs: [(1,a),(2,b)] in event_defs and [a,b] in pm.
        """
        self._init_storage_unit(storage_params.sort_storage, sorting_key, relation_op, equation_side,
                                storage_params.clean_expired_every, sort_by_first_timestamp)
        left_key, right_key, relop = None, None, None
        left_sort_by_first_timestamp, right_sort_by_first_timestamp = False, False
        # finding sorting keys in case user requested to sort by conditon
        if storage_params.sort_by_condition:
            left_key, right_key, relop = self._get_condition_based_sorting_keys(storage_params.attributes_priorities)
        # in case sorting by condition is impossible we sort by timestamp
        if relop is None:
            left_key, right_key, relop, left_sort_by_first_timestamp, right_sort_by_first_timestamp = self._get_sequence_based_sorting_keys()
        assert relop is not None
        self._left_subtree.create_storage_unit(storage_params, left_key, relop, EquationSides.left, left_sort_by_first_timestamp)
        self._right_subtree.create_storage_unit(storage_params, right_key, relop, EquationSides.right, right_sort_by_first_timestamp)
