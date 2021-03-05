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

    def __init__(self, height=0):
        self.height = height

    def get_leaves(self):
        """
        Returns all leaves in this tree - to be implemented by subclasses.
        """
        raise NotImplementedError()

    def get_node_copy(self):
        """
        Returns node copy instance
        """
        raise NotImplementedError()

class TreePlanLeafNode(TreePlanNode):
    """
    Represents a leaf of a tree-based plan.
    """

    def __init__(self, event_index: int, event_type: str = None, event_name: str = None, height=0):
        super().__init__(height)
        self.event_index = event_index
        self.event_type = event_type
        self.event_name = event_name
    def __str__(self) -> str:
        """
        “informal” or nicely printable string representation of an object
        """
        return str(hash(self))

    def get_leaves(self):
        assert self.height == 0
        return [self]

    def get_node_copy(self):
        node = TreePlanLeafNode(self.event_index, self.event_type, self.event_name, self.height)
        return node

class TreePlanInternalNode(TreePlanNode):
    """
    Represents an internal node of a tree-based plan.
    """

    def __init__(self, operator: OperatorTypes, height=0):
        super().__init__(height)
        self.operator = operator

    def get_operator(self):
        return self.operator

    def __str__(self) -> str:
        """
        “informal” or nicely printable string representation of an object
        """
        return str(self.operator).split('.')[1] + "-" + str(hash(self))

class TreePlanUnaryNode(TreePlanInternalNode):
    """
    Represents an internal unary node of a tree-based plan.
    """

    def __init__(self, operator: OperatorTypes, child: TreePlanNode):
        super().__init__(operator)
        self.height = self.child.height + 1
        self.child = child

    def get_leaves(self):
        return self.child.get_leaves()

    def get_node_copy(self):
        child = self.child.get_node_copy()
        node = TreePlanUnaryNode(self.operator, child)
        return node

class TreePlanBinaryNode(TreePlanInternalNode):
    """
    Represents an internal binary node of a tree-based plan.
    """

    def __init__(self, operator: OperatorTypes, left_child: TreePlanNode, right_child: TreePlanNode):
        super().__init__(operator, height=max(left_child.height,right_child.height)+1)
        self.left_child = left_child
        self.right_child = right_child

    def get_leaves(self):
        return self.left_child.get_leaves() + self.right_child.get_leaves()

    def get_node_copy(self):
        left_child = self.left_child.get_node_copy()
        right_child = self.right_child.get_node_copy()
        node = TreePlanBinaryNode(self.operator, left_child, right_child)
        return node

class TreePlan:
    """
    A complete tree-based evaluation plan.
    """

    def __init__(self, root: TreePlanNode):
        self.root = root
        self.height = root.height

