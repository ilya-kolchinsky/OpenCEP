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
        self.original_event_index = event_index  # Keeps the node index in the nested subtree its in.
        self.event_index = event_index  # The index of the node in the main tree.
        self.event_type = event_type
        self.event_name = event_name

    def get_leaves(self):
        return [self]

class TreePlanNestedNode(TreePlanNode):
    """
    This node is a fake leaf node when planning a tree with nested operators.
    It holds all the nested information needed later.
        -   sub_tree_plan is its nested tree plan
        -   args are all the args under this composite/kleene operator
        -   The cost is the cost of this whole subtree (including nested subtrees under this one)
        -   nested event index which is used to guarantee the correctness of sequence order verification
    """
    def __init__(self, event_index: int, tree_plan: TreePlanNode, args, cost):
        self.sub_tree_plan = tree_plan
        self.args = args
        self.cost = cost
        self.nested_event_index = event_index


class TreePlanInternalNode(TreePlanNode, ABC):
    """
    Represents an internal node of a tree-based plan.
    """
    def __init__(self, operator: OperatorTypes):
        self.operator = operator


class TreePlanUnaryNode(TreePlanInternalNode):
    """
    Represents an internal unary node of a tree-based plan.
    """
    def __init__(self, operator: OperatorTypes, child: TreePlanNode, index: int = 0):
        super().__init__(operator)
        self.child = child
        self.index = index

    def get_leaves(self):
        return self.child.get_leaves()


class TreePlanKCNode(TreePlanUnaryNode):
    """
    Represents an internal unary node of a tree-based plan.
    """
    def __init__(self, child: TreePlanNode, index: int, min_size: int, max_size: int):
        super().__init__(OperatorTypes.KC, child, index)
        self.min_size = min_size
        self.max_size = max_size


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


class TreePlanNegativeBinaryNode(TreePlanBinaryNode):
    """
    Represents a negative node of a tree-based plan.
    """
    def __init__(self, operator: OperatorTypes, positive_child: TreePlanNode, negative_child: TreePlanNode,
                 is_unbounded: bool):
        super().__init__(operator, positive_child, negative_child)
        self.is_unbounded = is_unbounded


class TreePlan:
    """
    A complete tree-based evaluation plan.
    """
    def __init__(self, root: TreePlanNode):
        self.root = root
