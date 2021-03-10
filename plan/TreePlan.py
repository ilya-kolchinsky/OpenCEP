"""
This file defines classes for specifying tree-structured complex event processing plans to be used by evaluation
mechanisms based on tree structure.
"""
from abc import ABC
from enum import Enum


class OperatorTypes(Enum):
    """
    Types of operators supported by a tree-based CEP evaluation plan.
    """
    AND = 0,
    SEQ = 1,
    OR = 2,
    NAND = 3,
    NSEQ = 4,
    KC = 5,


class TreePlanNode(ABC):
    """
    Represents a single node of a tree-based plan.
    """
    def get_leaves(self):
        """
        Returns all leaves in this tree - to be implemented by subclasses.
        """
        raise NotImplementedError()


class TreePlanLeafNode(TreePlanNode):
    """
    Represents a leaf of a tree-based plan.
    """

    def __init__(self, event_index: int, event_type: str = None, event_name: str = None):
        self.event_index = event_index
        self.event_type = event_type
        self.event_name = event_name

    def get_leaves(self):
        return [self]


class TreePlanInternalNode(TreePlanNode):
    """
    Represents an internal node of a tree-based plan.
    """
    def __init__(self, operator: OperatorTypes):
        self.operator = operator

    def get_operator(self):
        return self.operator


class TreePlanUnaryNode(TreePlanInternalNode):
    """
    Represents an internal unary node of a tree-based plan.
    """
    def __init__(self, operator: OperatorTypes, child: TreePlanNode):
        super().__init__(operator)
        self.child = child

    def get_leaves(self):
        return self.child.get_leaves()


class TreePlanBinaryNode(TreePlanInternalNode):
    """
    Represents an internal binary node of a tree-based plan.
    """
    def __init__(self, operator: OperatorTypes, left_child: TreePlanNode, right_child: TreePlanNode):
        super().__init__(operator)
        self.left_child = left_child
        self.right_child = right_child

    def get_leaves(self):
        return self.left_child.get_leaves() + self.right_child.get_leaves()


class TreePlan:
    """
    A complete tree-based evaluation plan.
    """
    def __init__(self, root: TreePlanNode):
        self.root = root
