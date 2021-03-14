from plan.negation.NegationAlgorithm import *


class NaiveNegationAlgorithm(NegationAlgorithm):
    """
    This class represents the naive negation algorithm.
    """
    def _add_negative_part(self, pattern: Pattern, statistics: Dict, positive_tree_plan: TreePlanBinaryNode,
                           all_negative_indices: List[int], unbounded_negative_indices: List[int]):
        for i, negative_index in enumerate(all_negative_indices):
            is_unbounded = negative_index in unbounded_negative_indices
            temp_leaf_index = len(pattern.positive_structure.args) + i
            negation_operator_structure = pattern.full_structure.args[negative_index]
            event_structure = negation_operator_structure.arg
            current_leaf = TreePlanLeafNode(temp_leaf_index, event_structure.type, event_structure.name)
            positive_tree_plan = NegationAlgorithm._instantiate_negative_node(pattern, positive_tree_plan,
                                                                              current_leaf, is_unbounded)
        return positive_tree_plan
