from abc import ABC
from datetime import timedelta
from typing import List, Tuple

from base.Formula import Formula, RelopTypes, EquationSides
from base.PatternStructure import PrimitiveEventStructure
from tree.InternalNode import InternalNode
from tree.Node import Node
from tree.PatternMatchStorage import TreeStorageParameters


class UnaryNode(InternalNode, ABC):
    """
    Represents an internal tree node with a single child.
    """
    def __init__(self, sliding_window: timedelta, parent: Node = None,
                 event_defs: List[Tuple[int, PrimitiveEventStructure]] = None, child: Node = None):
        super().__init__(sliding_window, parent, event_defs)
        self._child = child

    def get_leaves(self):
        if self._child is None:
            raise Exception("Unary Node with no child")
        return self._child.get_leaves()

    def _propagate_condition(self, condition: Formula):
        self._child.apply_formula(condition)

    def set_subtree(self, child: Node):
        """
        Sets the child node of this node.
        """
        self._child = child
        self._event_defs = child.get_event_definitions()

    def create_storage_unit(self, storage_params: TreeStorageParameters, sorting_key: callable = None,
                            rel_op: RelopTypes = None, equation_side: EquationSides = None,
                            sort_by_first_timestamp: bool = False):
        self._init_storage_unit(storage_params, sorting_key, rel_op, equation_side)
        self._child.create_storage_unit(storage_params)
