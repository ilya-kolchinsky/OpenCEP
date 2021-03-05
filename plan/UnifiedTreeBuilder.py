"""
This file contains the implementations of algorithms constructing a left-deep tree-based evaluation mechanism.
"""
from copy import deepcopy
from typing import List, Dict

from base.Pattern import Pattern
from base.PatternStructure import CompositeStructure
from misc import DefaultConfig
from misc.DefaultConfig import DEFAULT_TREE_COST_MODEL
from plan.TreeCostModels import TreeCostModels
from plan.TreePlan import TreePlanLeafNode, TreePlan, TreePlanNode, TreePlanUnaryNode, \
    TreePlanBinaryNode
from plan.TreePlanBuilder import TreePlanBuilder
from plan.TreePlanBuilderOrders import TreePlanBuilderOrder
from tree.TreeVisualizationUtility import GraphVisualization


class UnifiedTreeBuilder(TreePlanBuilder):
    """
    A class for deep tree builders.
    """

    def __init__(self, cost_model_type: TreeCostModels = DEFAULT_TREE_COST_MODEL,
                 tree_plan_order_approach: TreePlanBuilderOrder = TreePlanBuilderOrder.LEFT_TREE):
        super(UnifiedTreeBuilder, self).__init__(cost_model_type)
        self.tree_plan_build_approach = tree_plan_order_approach
        self.trees_number_nodes_shared = 0

    @staticmethod
    def get_instance(cost_model_type: TreeCostModels = DEFAULT_TREE_COST_MODEL,
                     tree_plan_order_approach: TreePlanBuilderOrder = TreePlanBuilderOrder.LEFT_TREE):
        return UnifiedTreeBuilder(cost_model_type, tree_plan_order_approach)

    @staticmethod
    def create_ordered_tree_builders():
        """
            Creates a tree plan builders according to the building order
       """
        approaches = TreePlanBuilderOrder.list()
        builders_set = {tree_plan_order: UnifiedTreeBuilder.get_instance(tree_plan_order_approach=tree_plan_order) for
                        tree_plan_order in
                        approaches}
        return builders_set

    def _union_tree_plans(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan):
        """
        Transforms a raw data object representing a single event into a dictionary of objects, each corresponding
        to a single event attribute.
        """
        raise NotImplementedError()

    def visualize(self, visualize_data: TreePlanNode or Dict[Pattern, TreePlan], title=None,
                  visualize_flag=DefaultConfig.VISUALIZATION):
        if visualize_flag and isinstance(visualize_data, TreePlanNode):
            G = GraphVisualization(title)
            G.build_from_root_treePlan(visualize_data, node_level=visualize_data.height)
            G.visualize()

        if visualize_flag and isinstance(visualize_data, dict):
            G = GraphVisualization(title)
            for i, (_, tree_plan) in enumerate(visualize_data.items()):
                G.build_from_root_treePlan(tree_plan.root, node_level=tree_plan.root.height)
            G.visualize()

    @staticmethod
    def _sub_tree_size(root):
        """
        get the size of subtree
        """
        if root is None:
            return 0

        if isinstance(root, TreePlanLeafNode):
            return 1

        if isinstance(root, TreePlanBinaryNode):
            return 1 + UnifiedTreeBuilder._sub_tree_size(root.left_child) + UnifiedTreeBuilder._sub_tree_size(
                root.right_child)

        if isinstance(root, TreePlanUnaryNode):
            return 1 + UnifiedTreeBuilder._sub_tree_size(root.child)

        raise Exception("Unsupported tree plan node type")


    def _create_tree_topology(self, pattern: Pattern):
        """
        Invokes an algorithm (to be implemented by subclasses) that builds an evaluation order of the operands, and
        converts it into a left-deep tree topology.
        """
        order = UnifiedTreeBuilder._create_evaluation_order(pattern) if isinstance(pattern.positive_structure,
                                                                                   CompositeStructure) else [0]
        return self._order_to_tree_topology(order, pattern)

    def _order_to_tree_topology(self, order: List[int], pattern: Pattern):

        if self.tree_plan_build_approach == TreePlanBuilderOrder.LEFT_TREE:
            return UnifiedTreeBuilder._order_to_tree_topology_left(order, pattern)

        if self.tree_plan_build_approach == TreePlanBuilderOrder.RIGHT_TREE:
            return UnifiedTreeBuilder._order_to_tree_topology_right(order, pattern)

        if self.tree_plan_build_approach == TreePlanBuilderOrder.BALANCED_TREE:
            return UnifiedTreeBuilder._order_to_tree_topology_balanced(order, pattern)

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
        tree_topology1 = UnifiedTreeBuilder._order_to_tree_topology_balanced(order[0:len(order) // 2], pattern)
        tree_topology2 = UnifiedTreeBuilder._order_to_tree_topology_balanced(order[len(order) // 2:], pattern)
        tree_topology = UnifiedTreeBuilder._instantiate_binary_node(pattern, tree_topology1, tree_topology2)
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
            tree_topology = UnifiedTreeBuilder._instantiate_binary_node(pattern, tree_topology,
                                                                        TreePlanLeafNode(order[i]))
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
            tree_topology = UnifiedTreeBuilder._instantiate_binary_node(pattern, TreePlanLeafNode(order[i]),
                                                                        tree_topology)
        return tree_topology


    @staticmethod
    def _order_to_tree_topology_half_left_half_balanced(order: List[int], pattern: Pattern):
        """
        A helper method for converting a given order to a right-deep tree topology.
        """
        if len(order) <= 1:
            return TreePlanLeafNode(order[0])

        tree_topology1 = UnifiedTreeBuilder._order_to_tree_topology_left(order[0:len(order) // 2], pattern)
        tree_topology2 = UnifiedTreeBuilder._order_to_tree_topology_balanced(order[len(order) // 2:], pattern)
        tree_topology = UnifiedTreeBuilder._instantiate_binary_node(pattern, tree_topology1, tree_topology2)
        return tree_topology


    @staticmethod
    def get_pattern_leaves_dict(pattern_to_tree_plan_map: Dict[Pattern, TreePlan]):
        """
                A helper method for return a [pattern -> [ [treePlanNode, Event] ...]] mapper.
        """
        leaves_dict = {}
        for i, pattern in enumerate(pattern_to_tree_plan_map):
            tree_plan_leaves_pattern = pattern_to_tree_plan_map[pattern].root.get_leaves()
            pattern_args = pattern.positive_structure.get_args()
            pattern_event_size = len(pattern_args)
            leaves_dict[pattern] = {tree_plan_leaves_pattern[i]: pattern_args[tree_plan_leaves_pattern[i].event_index]
                                    for i in
                                    range(pattern_event_size)}
        return leaves_dict

    @staticmethod
    def get_condition_from_pattern_in_one_sub_tree(plan_node: TreePlanNode, pattern: Pattern, leaves_dict):

        leaves_in_plan_node_1 = plan_node.get_leaves()
        if leaves_in_plan_node_1 is None:
            return None

        pattern1_leaves, pattern1_events = list(zip(*list(leaves_dict.get(pattern).items())))

        event_indexes1 = list(map(lambda e: e.event_index, leaves_in_plan_node_1))
        plan_node_1_events = list(
            filter(lambda i: pattern1_leaves[i].event_index in event_indexes1, range(len(pattern1_leaves))))
        names1 = {pattern1_events[event_index].name for event_index in plan_node_1_events}
        return deepcopy(pattern.condition.get_condition_of(names1, get_kleene_closure_conditions=False,
                                                           consume_returned_conditions=False))


