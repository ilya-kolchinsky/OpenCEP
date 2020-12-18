from abc import ABC

from base.Pattern import Pattern
from base.PatternStructure import AndOperator, SeqOperator
from plan.TreeCostModel import TreeCostModelFactory
from plan.TreeCostModels import TreeCostModels
from plan.TreePlan import TreePlan, TreePlanNode, OperatorTypes, TreePlanBinaryNode, TreePlanLeafNode
from plan.NegationAlgorithmTypes import NegationAlgorithmTypes
from misc.StatisticsTypes import StatisticsTypes


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
            negative_events_num = len(pattern.negative_structure.get_args())
            order = [*range(len(pattern.positive_structure.get_args()), len(pattern.full_structure.get_args()))]
            if pattern.negation_algorithm == NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM \
                    and pattern.full_statistics is not None:
                # list of tuples, each tuple saves a pair of negative event and its arrival statistic
                idx_statistics_list = []
                for i in range(0, negative_events_num):
                    # find the arrival rate of the specific negative event
                    if pattern.statistics_type is StatisticsTypes.ARRIVAL_RATES:
                        i_neg_event_statistic = pattern.full_statistics[pattern.negative_structure.args[i].orig_idx]
                    if pattern.statistics_type is StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
                        i_neg_event_statistic = pattern.full_statistics[1][pattern.negative_structure.args[i].orig_idx]
                    idx_statistics_list.append((order[i], i_neg_event_statistic))
                # sorting the negative events according to the arrival rates statistic
                idx_statistics_list.sort(key=lambda x: x[1], reverse=True)
                # rearrange "order" negative events by the desired order (according to the statistics)
                for i in range(0, negative_events_num):
                    order[i] = idx_statistics_list[i][0]
            if pattern.negation_algorithm == NegationAlgorithmTypes.LOWEST_POSITION_NEGATION_ALGORITHM:
                # TODO
                pass
            for i in range(0, negative_events_num):
                tree_topology = TreePlanBuilder._instantiate_binary_node(pattern, tree_topology, TreePlanLeafNode(order[i]))
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
            if type(right_subtree) is TreePlanLeafNode:
                if right_subtree.event_index >= len(pattern.positive_structure.get_args()):
                    if isinstance(pattern_structure, AndOperator):
                        operator_type = OperatorTypes.NAND
                    else:
                        operator_type = OperatorTypes.NSEQ
        return TreePlanBinaryNode(operator_type, left_subtree, right_subtree)
