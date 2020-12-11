from abc import ABC
from datetime import timedelta
from typing import Dict, List

from base.Pattern import Pattern
from base.PatternStructure import AndOperator, SeqOperator, PatternStructure, UnaryStructure, PrimitiveEventStructure, CompositeStructure
from misc.ConsumptionPolicy import ConsumptionPolicy
from plan.TreeCostModel import TreeCostModelFactory
from plan.TreeCostModels import TreeCostModels
from plan.TreePlan import TreePlan, TreePlanNode, OperatorTypes, TreePlanBinaryNode, TreePlanLeafNode
from plan.multi.MultiPatternUnifiedTreePlanApproaches import MultiPatternTreePlanUnionApproaches
from tree.Tree import Tree
from tree.nodes.LeafNode import LeafNode
from tree.nodes.Node import Node


class TreePlanBuilder(ABC):
    """
    The base class for the builders of tree-based plans.
    """
    def __init__(self, cost_model_type: TreeCostModels):
        self.__cost_model = TreeCostModelFactory.create_cost_model(cost_model_type)

    def build_tree_plan(self, pattern: Pattern):
        """
        Creates a tree-based evaluation plan for the given pattern.
        """
        return TreePlan(self._create_tree_topology(pattern))

    def _create_tree_topology(self, pattern: Pattern):
        """
        An abstract method for creating the actual tree topology.
        """
        raise NotImplementedError()

    @staticmethod
    def _union_tree_plans(pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan, tree_plan_union_approach: MultiPatternTreePlanUnionApproaches):
        if tree_plan_union_approach == MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES:
            return TreePlanBuilder.share_leaves(pattern_to_tree_plan_map)
        else:
            raise Exception("Unsupported union algorithm, yet")

    def _get_plan_cost(self, pattern: Pattern, plan: TreePlanNode):
        """
        Returns the cost of a given plan for the given plan according to a predefined cost model.
        """
        return self.__cost_model.get_plan_cost(pattern, plan)

    @staticmethod
    def _instantiate_binary_node(pattern: Pattern, left_subtree: TreePlanNode, right_subtree: TreePlanNode):
        """
        A helper method for the subclasses to instantiate tree plan nodes depending on the operator.
        """
        pattern_structure = pattern.positive_structure
        if isinstance(pattern_structure, AndOperator):
            operator_type = OperatorTypes.AND
        elif isinstance(pattern_structure, SeqOperator):
            operator_type = OperatorTypes.SEQ
        else:
            raise Exception("Unsupported binary operator")
        return TreePlanBinaryNode(operator_type, left_subtree, right_subtree)

    @staticmethod
    def share_leaves(pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan):

        leaves_dict = {}
        first_pattern = list(pattern_to_tree_plan_map.keys())[0]
        tree_plan_node_to_node_map = {}
        for i,pattern in enumerate(pattern_to_tree_plan_map):
            # pattern = list(pattern_to_tree_plan_map.keys())[0]
            tree_plan = pattern_to_tree_plan_map[pattern]
            leaves_dict[pattern] = TreePlanBuilder.tree_get_leaves(pattern.positive_structure, tree_plan.root,
                                           TreePlanBuilder.__get_operator_arg_list(pattern.positive_structure),
                                           pattern.window, None, pattern.consumption_policy)

        key_1 = list(leaves_dict[first_pattern].keys())[0]
        tree_plan_node  = leaves_dict[first_pattern][key_1]
        pass

    @staticmethod
    def __get_operator_arg_list(operator: PatternStructure):
        """
        Returns the list of arguments of the given operator for the tree construction process.
        """
        if isinstance(operator, CompositeStructure):
            return operator.args
        if isinstance(operator, UnaryStructure):
            return [operator.arg]
        # a PrimitiveEventStructure
        return [operator]

    @staticmethod
    def tree_get_leaves(root_operator: PatternStructure, tree_plan: TreePlanNode,
                        args: List[PatternStructure], sliding_window: timedelta, parent: Node, consumption_policy: ConsumptionPolicy,  leaves_nodes = {}):

        leaves_nodes = {}
        if tree_plan is None:
            return leaves_nodes

        if type(tree_plan) == TreePlanLeafNode:
            # a special case where the top operator of the entire pattern is an unary operator
            leaves_nodes[tree_plan] = LeafNode(sliding_window, tree_plan.event_index, root_operator.args[tree_plan.event_index], None)
            return leaves_nodes
            # return [constructed_node]
        ## (tree_plan, root_operator,   sliding_window, parent, consumption_policy)
        leaves_nodes = TreePlanBuilder.tree_get_leaves(root_operator, tree_plan.left_child, args, sliding_window, None, consumption_policy,leaves_nodes)
        leaves_nodes.update(TreePlanBuilder.tree_get_leaves(root_operator, tree_plan.right_child, args, sliding_window, None, consumption_policy, leaves_nodes))

        return leaves_nodes

