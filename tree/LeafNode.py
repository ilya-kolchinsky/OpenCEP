from datetime import timedelta
from typing import List

from base.Event import Event
from base.Formula import Formula, RelopTypes, EquationSides
from base.PatternStructure import QItem
from tree.Node import Node
from tree.PatternMatchStorage import TreeStorageParameters, SortedPatternMatchStorage


class LeafNode(Node):
    """
    A leaf node is responsible for a single event type of the pattern.
    """
    def __init__(self, sliding_window: timedelta, leaf_index: int, leaf_qitem: QItem, parent: Node):
        super().__init__(sliding_window, parent)
        self.__leaf_index = leaf_index
        self.__event_name = leaf_qitem.name
        self.__event_type = leaf_qitem.type

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

    def get_event_name(self):
        """
        Returns the name of events processed by this leaf.
        """
        return self.__event_name

    def get_leaf_index(self):
        """
        Returns the index of this leaf.
        """
        return self.__leaf_index

    def set_leaf_index(self, index: int):
        """
        Sets the index of this leaf.
        """
        self.__leaf_index = index

    def handle_event(self, event: Event):
        """
        Inserts the given event to this leaf.
        """
        self.clean_expired_partial_matches(event.timestamp)
        self._validate_and_propagate_partial_match([event])

    def _validate_new_match(self, events_for_new_match: List[Event]):
        """
        Validates the condition stored in this node on the given set of events.
        """
        if not super()._validate_new_match(events_for_new_match):
            return False
        binding = {self.__event_name: events_for_new_match[0].payload}
        return self._condition.eval(binding)

    def create_storage_unit(self, storage_params: TreeStorageParameters, sorting_key: callable = None,
                            rel_op: RelopTypes = None, equation_side: EquationSides = None,
                            sort_by_first_timestamp: bool = False):
        """
        For leaf nodes, we always want to create a sorted storage, since the events arrive in their natural order
        of occurrence anyway. Hence, a sorted storage is initialized either according to a user-specified key, or an
        arrival order if no storage parameters were explicitly specified.
        """
        should_use_default_storage_mode = not storage_params.sort_storage or sorting_key is None
        actual_sorting_key = (lambda pm: pm.events[0].timestamp) if should_use_default_storage_mode else sorting_key
        actual_sort_by_first_timestamp = should_use_default_storage_mode or sort_by_first_timestamp
        self._partial_matches = SortedPatternMatchStorage(actual_sorting_key, rel_op, equation_side,
                                                          storage_params.clean_up_interval,
                                                          actual_sort_by_first_timestamp, True)

    def get_structure_summary(self):
        return self.__event_name
