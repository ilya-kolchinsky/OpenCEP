from abc import ABC
from datetime import timedelta
from typing import List

from base.Event import Event
from base.Formula import RelopTypes, EquationSides
from tree.Node import Node, PrimitiveEventDefinition
from tree.PatternMatchStorage import TreeStorageParameters, UnsortedPatternMatchStorage, SortedPatternMatchStorage


class InternalNode(Node, ABC):
    """
    This class represents a non-leaf node of an evaluation tree.
    """
    def __init__(self, sliding_window: timedelta, parent: Node = None,
                 event_defs: List[PrimitiveEventDefinition] = None):
        super().__init__(sliding_window, parent)
        self._event_defs = event_defs

    def get_event_definitions(self):
        return self._event_defs

    def _validate_new_match(self, events_for_new_match: List[Event]):
        """
        Validates the condition stored in this node on the given set of events.
        """
        if not super()._validate_new_match(events_for_new_match):
            return False
        binding = {
            self._event_defs[i].name: events_for_new_match[i].payload for i in range(len(self._event_defs))
        }
        return self._condition.eval(binding)

    def _init_storage_unit(self, storage_params: TreeStorageParameters, sorting_key: callable = None,
                           rel_op: RelopTypes = None, equation_side: EquationSides = None,
                           sort_by_first_timestamp: bool = False):
        """
        An auxiliary method for setting up the storage of an internal node.
        In the internal nodes, we only sort the storage if a storage key is explicitly provided by the user.
        """
        if not storage_params.sort_storage or sorting_key is None:
            self._partial_matches = UnsortedPatternMatchStorage(storage_params.clean_up_interval)
        else:
            self._partial_matches = SortedPatternMatchStorage(sorting_key, rel_op, equation_side,
                                                              storage_params.clean_up_interval, sort_by_first_timestamp)

    def handle_new_partial_match(self, partial_match_source: Node):
        """
        A handler for a notification regarding a new partial match generated at one of this node's children.
        """
        raise NotImplementedError()
