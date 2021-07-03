from plan.negation.NegationAlgorithm import *
from plan.negation.StatisticNegationAlgorithm import StatisticNegationAlgorithm
from plan.TreePlan import TreePlanBinaryNode


class LowestPositionNegationAlgorithm(StatisticNegationAlgorithm):
    """
    This class implements the lowest position negation algorithm.
    """
    def _add_negative_part(self, pattern: Pattern, statistics: Dict, positive_tree_plan: TreePlanBinaryNode,
                           all_negative_indices: List[int], unbounded_negative_indices: List[int],
                           negative_index_to_tree_plan_node: Dict[int, TreePlanNode],
                           negative_index_to_tree_plan_cost: Dict[int, float]):
        initial_negative_tree_plan = super()._add_negative_part(pattern, statistics, positive_tree_plan,
                                                                all_negative_indices, unbounded_negative_indices,
                                                                negative_index_to_tree_plan_node,
                                                                negative_index_to_tree_plan_cost)
        # TODO: try to find a lower position for each bounded negative event
        return initial_negative_tree_plan
