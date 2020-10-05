from typing import List, Tuple

from base.PatternMatch import PatternMatch
from tree.UnaryNode import UnaryNode, Node
from base.PatternStructure import PrimitiveEventStructure



class ParallelUnaryNode(UnaryNode): # new root
    def __init__(self, sliding_window, parent: Node = None,
                 event_defs: List[Tuple[int, PrimitiveEventStructure]] = None, child: Node = None):
        super().__init__(sliding_window, parent, event_defs, child)

    def get_child(self):
        return self._child

    def get_leaves(self):
        #if self._is_root:
        return self._child.get_leaves()
        #else:
         #   return [self]

    def clean_expired_partial_matches(self, last_timestamp):
        return

    def get_event_definitions(self):
        return self.get_child().get_event_definitions()

    def handle_new_partial_match(self, partial_match_source: Node):
        new_partial_match = partial_match_source.get_last_unhandled_partial_match()
        super()._add_partial_match(new_partial_match)

    def handle_event(self, pm: PatternMatch):

        # self.clean_expired_partial_matches(event.timestamp)
        self._validate_and_propagate_partial_match(pm.events)