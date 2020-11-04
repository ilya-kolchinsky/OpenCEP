from typing import List

from base.Event import Event
from condition.Condition import RelopTypes, EquationSides
from misc.Utils import merge, merge_according_to, is_sorted
from tree.nodes.BinaryNode import BinaryNode
from tree.nodes.Node import PrimitiveEventDefinition
from tree.PatternMatchStorage import TreeStorageParameters


class SeqNode(BinaryNode):
    """
    An internal node representing a "SEQ" (sequence) operator.
    In addition to checking the time window and condition like the basic node does, SeqNode also verifies the order
    of arrival of the events in the partial matches it constructs.
    """
    def _set_event_definitions(self,
                               left_event_defs: List[PrimitiveEventDefinition],
                               right_event_defs: List[PrimitiveEventDefinition]):
        self._event_defs = merge(left_event_defs, right_event_defs, key=lambda x: x.index)

    def _merge_events_for_new_match(self,
                                    first_event_defs: List[PrimitiveEventDefinition],
                                    second_event_defs: List[PrimitiveEventDefinition],
                                    first_event_list: List[Event],
                                    second_event_list: List[Event]):
        return merge_according_to(first_event_defs, second_event_defs,
                                  first_event_list, second_event_list, key=lambda x: x.index)

    def _validate_new_match(self, events_for_new_match: List[Event]):
        if not is_sorted(events_for_new_match, key=lambda x: x.timestamp):
            return False
        return super()._validate_new_match(events_for_new_match)

    def get_structure_summary(self):
        return ("Seq",
                self._left_subtree.get_structure_summary(),
                self._right_subtree.get_structure_summary())

    def is_equivalent(self, other):
        """
        In addition to the checks performed by the base class, validates that the two nodes enforce the same sequence
        order.
        """
        if not super().is_equivalent(other):
            return False

        first_event_defs = self.get_event_definitions()
        second_event_defs = other.get_event_definitions()

        if len(first_event_defs) != len(second_event_defs):
            return False

        # we are assuming that the event definitions in a node are ordered by index,
        # which is a legitimate assumption due to the implementation of set_event_definitions
        for i in range(len(first_event_defs)):
            first_event_type = first_event_defs[i].type
            second_event_type = second_event_defs[i].type
            if first_event_type != second_event_type:
                return False
        return True

    def _get_sequence_based_sorting_keys(self):
        """
        Calculates the sorting keys according to the pattern sequence order and the user-provided priorities.
        """
        left_event_defs = self._left_subtree.get_event_definitions()
        right_event_defs = self._right_subtree.get_event_definitions()
        # comparing min and max leaf index of two subtrees
        min_left = min(left_event_defs, key=lambda x: x.index).index  # [ { ] } or [ { } ]
        max_left = max(left_event_defs, key=lambda x: x.index).index  # { [ } ] or { [ ] }
        min_right = min(right_event_defs, key=lambda x: x.index).index  # [ ] { }
        max_right = max(right_event_defs, key=lambda x: x.index).index  # { } [ ]
        if max_left < min_right:  # 3)
            left_sort, right_sort, rel_op = -1, 0, RelopTypes.SmallerEqual
        elif max_right < min_left:  # 4)
            left_sort, right_sort, rel_op = 0, -1, RelopTypes.GreaterEqual
        elif min_left < min_right:  # 1)
            left_sort, right_sort, rel_op = 0, 0, RelopTypes.SmallerEqual
        elif min_right < min_left:  # 2)
            left_sort, right_sort, rel_op = 0, 0, RelopTypes.GreaterEqual
        if rel_op is None:
            raise Exception("rel_op is None, something bad has happened")
        left_sorting_key = lambda pm: pm.events[left_sort].timestamp
        right_sorting_key = lambda pm: pm.events[right_sort].timestamp
        # left/right_sort == 0 means that left/right subtree will be sorted by first timestamp
        return left_sorting_key, right_sorting_key, rel_op, (left_sort == 0), (right_sort == 0)

    def create_storage_unit(self, storage_params: TreeStorageParameters, sorting_key: callable = None,
                            rel_op: RelopTypes = None, equation_side: EquationSides = None,
                            sort_by_first_timestamp: bool = False):
        """
        This function creates the storage for partial_matches it gives a special key: callable
        to the storage unit which tells the storage unit on which attribute(only timestamps here)
        to sort.
        We assume all events are in SEQ(,,,,...) which makes the order in partial match the same
        as in event_defs: [(1,a),(2,b)] in event_defs and [a,b] in pm.
        """
        self._init_storage_unit(storage_params, sorting_key, rel_op, equation_side, sort_by_first_timestamp)
        if not storage_params.sort_storage:
            # efficient storage is disabled
            self._left_subtree.create_storage_unit(storage_params)
            self._right_subtree.create_storage_unit(storage_params)
            return
        left_sort_by_first_timestamp, right_sort_by_first_timestamp = False, False
        # finding sorting keys in case user requested to sort by condition
        if storage_params.prioritize_sorting_by_timestamp:
            # first try the timestamps, then the conditions
            left_key, right_key, left_rel_op, left_sort_by_first_timestamp, right_sort_by_first_timestamp = \
                self._get_sequence_based_sorting_keys()
            if left_rel_op is None:
                left_key, left_rel_op, left_equation_size, right_key, right_rel_op, right_equation_size = \
                    self._get_condition_based_sorting_keys(storage_params.attributes_priorities)
            else:
                right_rel_op, left_equation_size, right_equation_size = \
                    left_rel_op, EquationSides.left, EquationSides.right
        else:
            # first try the conditions, then the timestamps
            left_key, left_rel_op, left_equation_size, right_key, right_rel_op, right_equation_size = \
                self._get_condition_based_sorting_keys(storage_params.attributes_priorities)
            if left_rel_op is None:
                left_key, right_key, left_rel_op, left_sort_by_first_timestamp, right_sort_by_first_timestamp = \
                    self._get_sequence_based_sorting_keys()
                right_rel_op, left_equation_size, right_equation_size = \
                    left_rel_op, EquationSides.left, EquationSides.right
        if left_rel_op is None:
            # both sequence-based and condition-based initialization failed
            raise Exception("Should never happen")
        self._left_subtree.create_storage_unit(storage_params, left_key, left_rel_op, left_equation_size,
                                               left_sort_by_first_timestamp)
        self._right_subtree.create_storage_unit(storage_params, right_key, right_rel_op, right_equation_size,
                                                right_sort_by_first_timestamp)
