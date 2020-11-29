from abc import ABC

from base.Pattern import Pattern
from base.PatternStructure import AndOperator, SeqOperator, NegationOperator
from plan.TreeCostModel import TreeCostModelFactory
from plan.TreeCostModels import TreeCostModels
from plan.TreePlan import TreePlan, TreePlanNode, OperatorTypes, TreePlanBinaryNode, TreePlanLeafNode


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

    def _get_plan_cost(self, pattern: Pattern, plan: TreePlanNode):
        """
        Returns the cost of a given plan for the given plan according to a predefined cost model.
        """
        return self.__cost_model.get_plan_cost(pattern, plan)

    @staticmethod
    def _add_negative_part(positive_subtree: TreePlanBinaryNode, pattern: Pattern):
        """
        This method adds the negative part to the tree plan (that includes only the positive part),
        according to the negation algorithm the user chose
        """
        tree_topology = positive_subtree
        if pattern.negative_structure is not None:
            for i in range(len(pattern.positive_structure.get_args()), len(pattern.combined_pos_neg_structure.get_args())):
                tree_topology = TreePlanBuilder._instantiate_binary_node(pattern, tree_topology, TreePlanLeafNode(i))
        return tree_topology

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
        if pattern.negative_structure is not None:
            if type(pattern.combined_pos_neg_structure.args[right_subtree.event_index]) == NegationOperator:
                if isinstance(pattern_structure, AndOperator):
                    operator_type = OperatorTypes.NAND
                else:
                    operator_type = OperatorTypes.NSEQ
        return TreePlanBinaryNode(operator_type, left_subtree, right_subtree)
