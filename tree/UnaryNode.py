from abc import ABC
from datetime import timedelta
from typing import List

from base.Formula import Formula, RelopTypes, EquationSides
from tree.InternalNode import InternalNode
from tree.Node import Node, PrimitiveEventDefinition
from tree.PatternMatchStorage import TreeStorageParameters


class UnaryNode(InternalNode, ABC):
    """
    Represents an internal tree node with a single child.
    """
    def __init__(self, sliding_window: timedelta, parents: List[Node] = None,
                 event_defs: List[PrimitiveEventDefinition] = None, child: Node = None, pattern_id=0):
        super().__init__(sliding_window, parents, event_defs, pattern_id)
        self._child = child

    def get_leaves(self):
        if self._child is None:
            raise Exception("Unary Node with no child")
        return self._child.get_leaves()

    def get_child(self):
        return self._child

    def _propagate_condition(self, condition: Formula):
        self._child.apply_formula(condition)

    def set_subtree(self, child: Node):
        """
        Sets the child node of this node.
        """
        self._child = child
        self._event_defs = child.get_event_definitions()

    def update_sliding_window(self, sliding_window: timedelta):
        self.set_sliding_window(sliding_window)
        self._child.update_sliding_window(sliding_window)

    def replace_subtree(self, child: Node):
        self.set_subtree(child)
        child.add_parent(self)

    def create_parent_to_info_dict(self):
        if self._child:
            self._child.create_parent_to_info_dict()
        if self._parents:
            # we call this method before we share nodes so each node has only one parent (or none).
            if len(self._parents) > 1:
                raise Exception("This method should not be called when there is more than one parent.")
            self.add_to_dict(self._parents[0], self._event_defs)

    def create_storage_unit(self, storage_params: TreeStorageParameters, sorting_key: callable = None,
                            rel_op: RelopTypes = None, equation_side: EquationSides = None,
                            sort_by_first_timestamp: bool = False):
        self._init_storage_unit(storage_params, sorting_key, rel_op, equation_side)
        self._child.create_storage_unit(storage_params)
