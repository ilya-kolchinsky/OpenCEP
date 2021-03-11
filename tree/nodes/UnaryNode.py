from abc import ABC
from datetime import timedelta
from typing import List, Set

from condition.Condition import Condition, RelopTypes, EquationSides
from tree.nodes.InternalNode import InternalNode
from tree.nodes.Node import Node, PrimitiveEventDefinition, PatternParameters
from tree.PatternMatchStorage import TreeStorageParameters


class UnaryNode(InternalNode, ABC):
    """
    Represents an internal tree node with a single child.
    """
    def __init__(self, pattern_params: PatternParameters, parents: List[Node] = None, pattern_ids: int or Set[int] = None,
                 event_defs: List[PrimitiveEventDefinition] = None, child: Node = None):
        super().__init__(pattern_params, parents, pattern_ids, event_defs)
        self._child = child

    def get_leaves(self):
        if self._child is None:
            raise Exception("Unary Node with no child")
        return self._child.get_leaves()

    def _propagate_condition(self, condition: Condition):
        self._child.apply_condition(condition)

    def set_subtree(self, child: Node):
        """
        Sets the child node of this node.
        """
        self._child = child
        # only the positive child definitions should be applied on this node
        self._event_defs = child.get_positive_event_definitions()

    def _propagate_pattern_parameters(self, pattern_params: PatternParameters):
        self._child.set_and_propagate_pattern_parameters(pattern_params)

    def propagate_pattern_id(self, pattern_id: int):
        self.add_pattern_ids({pattern_id})
        self._child.propagate_pattern_id(pattern_id)

    def replace_subtree(self, child: Node):
        """
        Replaces the child of this node with the given node.
        """
        self.set_subtree(child)
        child.add_parent(self)

    def create_parent_to_info_dict(self):
        if self._child is not None:
            self._child.create_parent_to_info_dict()
        super().create_parent_to_info_dict()

    def create_storage_unit(self, storage_params: TreeStorageParameters, sorting_key: callable = None,
                            rel_op: RelopTypes = None, equation_side: EquationSides = None,
                            sort_by_first_timestamp: bool = False):
        self._init_storage_unit(storage_params, sorting_key, rel_op, equation_side)
        self._child.create_storage_unit(storage_params)

    def get_child(self):
        """
        Returns the child of this unary node.
        """
        return self._child

    def is_equivalent(self, other):
        """
        In addition to the checks performed by the base class, verifies the equivalence of the child nodes.
        """
        return super().is_equivalent(other) and self._child.is_equivalent(other.get_child())
