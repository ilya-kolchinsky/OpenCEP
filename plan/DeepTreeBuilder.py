"""
This file contains the implementations of algorithms constructing a left-deep tree-based evaluation mechanism.
"""
from typing import List

from base.Pattern import Pattern
from base.PatternStructure import CompositeStructure
from misc.DefaultConfig import DEFAULT_TREE_COST_MODEL
from plan.TreeCostModels import TreeCostModels
from plan.TreePlan import TreePlanLeafNode
from plan.TreePlanBuilder import TreePlanBuilder
from plan.TreePlanBuilderOrders import TreePlanBuilderOrder


class DeepTreeBuilder(TreePlanBuilder):
    """
    An abstract class for left-deep tree builders.
    """

    def __init__(self, cost_model_type: TreeCostModels = DEFAULT_TREE_COST_MODEL, tree_plan_order_approach: TreePlanBuilderOrder = TreePlanBuilderOrder.LEFT_TREE):
        super(DeepTreeBuilder, self).__init__(cost_model_type)
        self.tree_plan_build_approach = tree_plan_order_approach

    def _create_tree_topology(self, pattern: Pattern):
        """
        Invokes an algorithm (to be implemented by subclasses) that builds an evaluation order of the operands, and
        converts it into a left-deep tree topology.
        """
        order = DeepTreeBuilder._create_evaluation_order(pattern) if isinstance(pattern.positive_structure, CompositeStructure) else [0]
        return self._order_to_tree_topology(order, pattern)

    def _order_to_tree_topology(self, order: List[int], pattern: Pattern):

        if self.tree_plan_build_approach == TreePlanBuilderOrder.LEFT_TREE:
            return DeepTreeBuilder._order_to_tree_topology_left(order, pattern)

        if self.tree_plan_build_approach == TreePlanBuilderOrder.RIGHT_TREE:
            return DeepTreeBuilder._order_to_tree_topology_right(order, pattern)

        if self.tree_plan_build_approach == TreePlanBuilderOrder.BALANCED_TREE:
            return DeepTreeBuilder._order_to_tree_topology_balanced(order, pattern)

        if self.tree_plan_build_approach == TreePlanBuilderOrder.HALF_LEFT_HALF_BALANCED_TREE:
            return DeepTreeBuilder._order_to_tree_topology_half_left_half_balanced(order, pattern)

        else:
            raise Exception("Unsupported tree topology build algorithm, yet")

    @staticmethod
    def _create_evaluation_order(pattern: Pattern):
        args_num = len(pattern.positive_structure.args)
        return list(range(args_num))

    @staticmethod
    def _order_to_tree_topology_balanced(order: List[int], pattern: Pattern):
        """
        A helper method for converting a given order to a balanced tree topology.
        """
        if len(order) <= 1:
            return TreePlanLeafNode(order[0])
        tree_topology1 = DeepTreeBuilder._order_to_tree_topology_balanced(order[0:len(order) // 2], pattern)
        tree_topology2 = DeepTreeBuilder._order_to_tree_topology_balanced(order[len(order) // 2:], pattern)
        tree_topology = TreePlanBuilder._instantiate_binary_node(pattern, tree_topology1, tree_topology2)
        return tree_topology

    @staticmethod
    def _order_to_tree_topology_left(order: List[int], pattern: Pattern):
        """
        A helper method for converting a given order to a left-deep tree topology.
        """
        if len(order) <= 1:
            return TreePlanLeafNode(order[0])
        tree_topology = TreePlanLeafNode(order[0])
        for i in range(1, len(order)):
            tree_topology = TreePlanBuilder._instantiate_binary_node(pattern, tree_topology, TreePlanLeafNode(order[i]))
        return tree_topology

    @staticmethod
    def _order_to_tree_topology_right(order: List[int], pattern: Pattern):
        """
        A helper method for converting a given order to a right-deep tree topology.
        """
        if len(order) <= 1:
            return TreePlanLeafNode(order[0])
        tree_topology = TreePlanLeafNode(order[len(order) - 1])
        for i in range(len(order) - 2, -1, -1):
            tree_topology = TreePlanBuilder._instantiate_binary_node(pattern, TreePlanLeafNode(order[i]), tree_topology)
        return tree_topology

    @staticmethod
    def _order_to_tree_topology_half_left_half_balanced(order: List[int], pattern: Pattern):
        """
        A helper method for converting a given order to a right-deep tree topology.
        """
        if len(order) <= 1:
            return TreePlanLeafNode(order[0])

        tree_topology1 = DeepTreeBuilder._order_to_tree_topology_left(order[0:len(order) // 2], pattern)
        tree_topology2 = DeepTreeBuilder._order_to_tree_topology_balanced(order[len(order) // 2:], pattern)
        tree_topology = TreePlanBuilder._instantiate_binary_node(pattern, tree_topology1, tree_topology2)
        return tree_topology
