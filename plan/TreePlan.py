"""
This file defines classes for specifying tree-structured complex event processing plans to be used by evaluation
mechanisms based on tree structure.
"""
from abc import ABC
from enum import Enum

from condition.CompositeCondition import CompositeCondition, AndCondition


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
    def __init__(self, condition: CompositeCondition = AndCondition()):
        self.condition = condition

    def get_event_names(self):
        """
        Returns all event names in this tree.
        """
        return [leaf.event_name for leaf in self.get_leaves()]

    def apply_condition(self, condition: CompositeCondition):
        """
        Applies the given condition on all nodes in the subtree of this node.
        The process of applying the condition is recursive and proceeds in a bottom-up manner - first the condition is
        propagated down the subtree, then sub-conditions for this node are assigned.
        """
        self._propagate_condition(condition)
        self.condition = condition.get_condition_of(set(self.get_event_names()), get_kleene_closure_conditions=False,
                                                    consume_returned_conditions=True)

    def is_equivalent(self, other):
        """
        Returns True if this node and the given node are equivalent and False otherwise.
        Two nodes are considered equivalent if they possess equivalent structures and the nodes of these structures
        contain equivalent conditions.
        This default implementation only compares the types and conditions of the nodes. The structure equivalence
        test must be implemented by the subclasses.
        """
        return type(self) == type(other) and self.condition == other.condition

    def get_leaves(self):
        """
        Returns all leaves in this tree - to be implemented by subclasses.
        """
        raise NotImplementedError()

    def _propagate_condition(self, condition: CompositeCondition):
        """
        Propagates the given condition to successors - to be implemented by subclasses.
        """
        raise NotImplementedError()


class TreePlanLeafNode(TreePlanNode):
    """
    Represents a leaf of a tree-based plan.
    """
    def __init__(self, event_index: int,
                 event_type: str = None, event_name: str = None, condition: CompositeCondition = AndCondition()):
        super().__init__(condition)
        self.original_event_index = event_index  # Keeps the node index in the nested subtree its in.
        self.event_index = event_index  # The index of the node in the main tree.
        self.event_type = event_type
        self.event_name = event_name

    def get_leaves(self):
        return [self]

    def _propagate_condition(self, condition: CompositeCondition):
        pass

    def is_equivalent(self, other):
        """
        Checks if the two nodes accept the same event type and have the same event name.
        TODO: should also work for equivalent nodes with different names.
        """
        return super().is_equivalent(other) and \
               self.event_type == other.event_type and self.event_name == other.event_name


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
        super().__init__()
        self.sub_tree_plan = tree_plan
        self.args = args
        self.cost = cost
        self.nested_event_index = event_index

    def get_leaves(self):
        return self.sub_tree_plan.get_leaves()

    def _propagate_condition(self, condition: CompositeCondition):
        self.sub_tree_plan.apply_condition(condition)

    def apply_condition(self, condition: CompositeCondition):
        """
        The base method is overridden since the condition of a nested node is expected to always be empty.
        """
        self._propagate_condition(condition)

    def is_equivalent(self, other):
        """
        In addition to the checks performed by the base class, verifies the equivalence of the nested plans.
        """
        return super().is_equivalent(other) and self.sub_tree_plan.is_equivalent(other.sub_tree_plan)


class TreePlanInternalNode(TreePlanNode, ABC):
    """
    Represents an internal node of a tree-based plan.
    """
    def __init__(self, operator: OperatorTypes, condition: CompositeCondition = AndCondition()):
        super().__init__(condition)
        self.operator = operator

    def is_equivalent(self, other):
        """
        In addition to the checks performed by the base class, verifies the equivalence of the operators.
        """
        return super().is_equivalent(other) and self.operator == other.operator


class TreePlanUnaryNode(TreePlanInternalNode):
    """
    Represents an internal unary node of a tree-based plan.
    """
    def __init__(self, operator: OperatorTypes, child: TreePlanNode, index: int = 0,
                 condition: CompositeCondition = AndCondition()):
        super().__init__(operator, condition)
        self.child = child
        self.index = index

    def get_leaves(self):
        return self.child.get_leaves()

    def _propagate_condition(self, condition: CompositeCondition):
        self.child.apply_condition(condition)

    def is_equivalent(self, other):
        """
        In addition to the checks performed by the base class, verifies the equivalence of the child nodes.
        """
        return super().is_equivalent(other) and self.child.is_equivalent(other.child)


class TreePlanKCNode(TreePlanUnaryNode):
    """
    Represents an internal unary node of a tree-based plan.
    """
    def __init__(self, child: TreePlanNode, index: int, min_size: int, max_size: int,
                 condition: CompositeCondition = AndCondition()):
        super().__init__(OperatorTypes.KC, child, index, condition)
        self.min_size = min_size
        self.max_size = max_size

    def apply_condition(self, condition: CompositeCondition):
        """
        The base method is overridden to extract KC conditions.
        """
        self._propagate_condition(condition)
        self.condition = condition.get_condition_of(set(self.get_event_names()), get_kleene_closure_conditions=True,
                                                    consume_returned_conditions=True)

    def is_equivalent(self, other):
        """
        In addition to the checks performed by the base class, compares the min_size and max_size fields.
        """
        if not super().is_equivalent(other):
            return False
        return self.min_size == other.min_size and self.max_size == other.max_size


class TreePlanBinaryNode(TreePlanInternalNode):
    """
    Represents an internal binary node of a tree-based plan.
    """
    def __init__(self, operator: OperatorTypes, left_child: TreePlanNode, right_child: TreePlanNode,
                 condition: CompositeCondition = AndCondition()):
        super().__init__(operator, condition)
        self.left_child = left_child
        self.right_child = right_child

    def get_leaves(self):
        return self.left_child.get_leaves() + self.right_child.get_leaves()

    def _propagate_condition(self, condition: CompositeCondition):
        self.left_child.apply_condition(condition)
        self.right_child.apply_condition(condition)

    def is_equivalent(self, other):
        """
        In addition to the checks performed by the base class, checks if:
        the left subtrees structures are equivalent and the right subtrees structures are equivalent OR
        the left of the first is equivalent to the right of the second and the right of the first is equivalent to
        the left of the second.
        """
        if not super().is_equivalent(other):
            return False
        v1 = self.left_child.is_equivalent(other.left_child)
        v2 = self.right_child.is_equivalent(other.right_child)
        if v1 and v2:
            return True
        v3 = self.left_child.is_equivalent(other.right_child)
        v4 = self.right_child.is_equivalent(other.left_child)
        return v3 and v4


class TreePlanNegativeBinaryNode(TreePlanBinaryNode):
    """
    Represents a negative node of a tree-based plan.
    """
    def __init__(self, operator: OperatorTypes, positive_child: TreePlanNode, negative_child: TreePlanNode,
                 is_unbounded: bool, condition: CompositeCondition = AndCondition()):
        super().__init__(operator, positive_child, negative_child, condition)
        self.is_unbounded = is_unbounded

    def is_equivalent(self, other):
        """
        In addition to the checks performed by the base class, separately verifies the equivalence of the positive and
        the negative subtrees.
        """
        if not TreePlanNode.is_equivalent(self, other):
            return False
        v1 = self.left_child.is_equivalent(other.left_child)
        v2 = self.right_child.is_equivalent(other.right_child)
        return v1 and v2


class TreePlan:
    """
    A complete tree-based evaluation plan.
    """
    def __init__(self, root: TreePlanNode):
        self.root = root
