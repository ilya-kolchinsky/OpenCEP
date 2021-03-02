"""
TODO
"""

from plan.TreePlanBuilder import TreePlanBuilder
from negationAlgorithms.NegationAlgorithm import *


class StatisticNegationAlgorithm(NegationAlgorithm):
    """
    TODO
    """

    def __init__(self, negation_algorithm_type: NegationAlgorithmTypes = NegationAlgorithmTypes.
                 STATISTIC_NEGATION_ALGORITHM):
        super().__init__(negation_algorithm_type)

    def add_negative_part(self, pattern: Pattern, positive_tree_plan: TreePlanBinaryNode):
        """
        TODO
        """
        tree_topology = positive_tree_plan
        if pattern.negative_structure is None or pattern.full_statistics is None:
            return tree_topology
        self.calculate_original_indices(pattern)
        negative_events_num = len(pattern.negative_structure.args)
        order = []
        indices_statistics_list = self.create_sorted_statistics_list(pattern)
        # Arrange "order" negative events by the desired order (according to the statistics)
        for i in range(0, negative_events_num):
            order.append(indices_statistics_list[i][0] + len(pattern.positive_structure.args))
            # The negative part being added to the tree plan
        for i in range(0, negative_events_num):
            tree_topology = TreePlanBuilder.instantiate_binary_node(pattern,
                                                                    tree_topology, TreePlanLeafNode(order[i]))
        self.adjust_tree_plan_indices(tree_topology, pattern)
        return tree_topology
