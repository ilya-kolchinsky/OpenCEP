from plan.TreePlanBuilder import TreePlanBuilder
from negationAlgorithms.NegationAlgorithm import *


class NaiveNegationAlgorithm(NegationAlgorithm):
    """
    This class represents the naive negation algorithm, and saves the data related to it.
    """
    def __init__(self, negation_algorithm_type: NegationAlgorithmTypes = DefaultConfig.DEFAULT_NEGATION_ALGORITHM):
        super().__init__(negation_algorithm_type)

    def add_negative_part(self, pattern: Pattern, positive_tree_plan: TreePlanBinaryNode):
        """
        This method adds the negative part to the tree plan (that includes only the positive part),
        according to the naive algorithm, i.e. - negative nodes inserted on top of the positive part, in the same
        order as in the pattern
        """
        tree_topology = positive_tree_plan
        if pattern.negative_structure is None:
            return tree_topology
        negative_events_num = len(pattern.negative_structure.args)
        order = [*range(len(pattern.positive_structure.args), len(pattern.full_structure.args))]
        for i in range(0, negative_events_num):
            tree_topology = TreePlanBuilder.instantiate_binary_node(pattern,
                                                                    tree_topology, TreePlanLeafNode(order[i]))
        self.calculate_original_indices(pattern)
        self.adjust_tree_plan_indices(tree_topology, pattern)
        return tree_topology
