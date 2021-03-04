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
    pass


class TreePlanLeafNode(TreePlanNode):
    """
    Represents a leaf of a tree-based plan.
    """
    def __init__(self, event_index: int, event_type: str = None, event_name: str = None):
        self.nested_event_index = event_index  # Keeps the node index in the nested subtree its in.
        self.event_index = event_index  # The index of the node in the main tree.
        self.event_type = event_type
        self.event_name = event_name


class TreePlanNestedNode(TreePlanNode):
    """
    This node is a fake leaf node when planning a tree with nested operators.
    It holds all the nested information needed later.
        -   sub_tree_plan is its nested tree plan.
        -   args are all the args under this composite/kleene operator
        -   The cost is the cost of this whole subtree (including nested subtrees under this one)
    """
    def __init__(self, event_index: int, tree_plan: TreePlanNode, args, cost):
        self.nested_event_index = event_index
        self.sub_tree_plan = tree_plan
        self.args = args
        self.cost = cost


class TreePlanInternalNode(TreePlanNode):
    """
    Represents an internal node of a tree-based plan.
    """
    def __init__(self, operator: OperatorTypes):
        self.operator = operator


class TreePlanUnaryNode(TreePlanInternalNode):
    """
    Represents an internal unary node of a tree-based plan.
    """
    def __init__(self, operator: OperatorTypes, child: TreePlanNode):
        super().__init__(operator)
        self.child = child


class TreePlanBinaryNode(TreePlanInternalNode):
    """
    Represents an internal binary node of a tree-based plan.
    """
    def __init__(self, operator: OperatorTypes, left_child: TreePlanNode, right_child: TreePlanNode):
        super().__init__(operator)
        self.left_child = left_child
        self.right_child = right_child


class TreePlan:
    """
    A complete tree-based evaluation plan.
    """
    def __init__(self, root: TreePlanNode):
        self.root = root
