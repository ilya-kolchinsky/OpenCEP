"""
This class contains the implementation of unary node used to connect different tree structures after split.
"""

from typing import List, Tuple
from base.PatternMatch import PatternMatch
from tree.UnaryNode import UnaryNode, Node
from tree.BinaryNode import BinaryNode
from base.PatternStructure import PrimitiveEventStructure


class ParallelUnaryNode(UnaryNode):
    def __init__(self, sliding_window, parent: Node = None, event_defs: List[Tuple[int,PrimitiveEventStructure]] = None,
                 child: Node = None):
        super().__init__(sliding_window, parent, event_defs, child)
        # this fields will contain the matches of this node in order to keep them from expiring
        self.our_pattern_matches = []
        self._set_event_definitions()

    def _set_event_definitions(self):
        self._event_defs = self.get_event_definitions()

    def get_child(self):
        return self._child

    def get_leaves(self):
        return self._child.get_leaves()

    def clean_expired_partial_matches(self, last_timestamp):
        return

    def get_event_definitions(self):
        return self.get_child().get_event_definitions()

    def handle_new_partial_match(self, partial_match_source: Node = None):
        new_partial_match = self.get_child().get_last_unhandled_partial_match()
        self.our_pattern_matches.append(new_partial_match)
        self._partial_matches.add(new_partial_match)

    def handle_event(self, pm: PatternMatch):
        self._validate_and_propagate_partial_match(pm.events)

    def get_our_matches(self):
        return self.our_pattern_matches

    def get_partial_matches(self, filter_value: int or float = None):
        return self._partial_matches

    def _add_partial_match(self, pm: PatternMatch):
        self._partial_matches.add(pm)
        if self._parent is not None:
            self._unhandled_partial_matches.put(pm)
            self._parent.handle_new_partial_match(self)

    def get_unary_children(self):
        # Here we want to get the closest children of self that are ParallelUnaryNode. According to how we built the
        # tree, it will always be its direct children. This is based solely on our splitting algorithm and will need
        # updating for other splitting algorithms
        child = self.get_child()
        children = []
        if isinstance(child, UnaryNode):
            node = child._child
            if isinstance(node, ParallelUnaryNode):
                children.append(node)
        elif isinstance(child, BinaryNode):
            left_child = child.get_left_subtree()
            right_child = child.get_right_subtree()
            if isinstance(left_child, ParallelUnaryNode):
                children.append(left_child)
            if isinstance(right_child, ParallelUnaryNode):
                children.append(right_child)
        return children
