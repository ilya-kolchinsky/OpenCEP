from plan.negation.NegationAlgorithm import *


class NaiveNegationAlgorithm(NegationAlgorithm):
    """
    This class represents the naive negation algorithm, and saves the data related to it.
    """
    def __init__(self):
        super().__init__(NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)

    def add_negative_part(self, pattern: Pattern, positive_tree_plan: TreePlanBinaryNode):
        """
        This method adds the negative part to the tree plan (that includes only the positive part),
        according to the naive algorithm, i.e. - negative nodes inserted on top of the positive part, in the same
        order as in the pattern
        """
        if pattern.negative_structure is None:
            return positive_tree_plan
        negative_events_num = len(pattern.negative_structure.args)
        order = [*range(len(pattern.positive_structure.args), len(pattern.full_structure.args))]
        for i in range(0, negative_events_num):
            positive_tree_plan = NegationAlgorithm._instantiate_negative_node(pattern,
                                                                              positive_tree_plan,
                                                                              TreePlanLeafNode(order[i]))
        self.calculate_original_indices(pattern)
        self.adjust_tree_plan_indices(positive_tree_plan, pattern)
        return positive_tree_plan
