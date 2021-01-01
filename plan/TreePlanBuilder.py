from abc import ABC
from typing import Dict

from base.Pattern import Pattern
from base.PatternStructure import AndOperator, SeqOperator
from misc import DefaultConfig
from plan.TreeCostModel import TreeCostModelFactory
from plan.TreeCostModels import TreeCostModels
from plan.TreePlan import TreePlan, TreePlanNode, OperatorTypes, TreePlanBinaryNode
from tree.TreeVisualizationUtility import GraphVisualization


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

    def visualize(self, visualize_data: TreePlanNode or Dict[Pattern, TreePlan], title=None, visualize_flag=DefaultConfig.VISUALIZATION):
        if visualize_flag and isinstance(visualize_data, TreePlanNode):
            G = GraphVisualization(title)
            G.build_from_root_treePlan(visualize_data, node_level=visualize_data.height)
            G.visualize()

        if visualize_flag and isinstance(visualize_data, dict):
            G = GraphVisualization(title)
            for i, (_, tree_plan) in enumerate(visualize_data.items()):
                G.build_from_root_treePlan(tree_plan.root, node_level=tree_plan.root.height)
            G.visualize()

    def _create_tree_topology(self, pattern: Pattern):
        """
        An abstract method for creating the actual tree topology.
        """
        raise NotImplementedError()

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
