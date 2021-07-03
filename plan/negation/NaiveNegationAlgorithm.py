from base.PatternStructure import CompositeStructure, UnaryStructure
from plan.negation.NegationAlgorithm import *


class NaiveNegationAlgorithm(NegationAlgorithm):
    """
    This class represents the naive negation algorithm.
    """
    def _add_negative_part(self, pattern: Pattern, statistics: Dict, positive_tree_plan: TreePlanBinaryNode,
                           all_negative_indices: List[int], unbounded_negative_indices: List[int],
                           negative_index_to_tree_plan_node: Dict[int, TreePlanNode],
                           negative_index_to_tree_plan_cost: Dict[int, float]):
        args = pattern.get_top_level_structure_args()
        for i, negative_index in enumerate(all_negative_indices):
            is_unbounded = negative_index in unbounded_negative_indices
            negation_operator_structure = args[negative_index]
            negation_operator_arg = negation_operator_structure.arg
            if negative_index in negative_index_to_tree_plan_node:
                assert isinstance(negation_operator_arg, CompositeStructure) or isinstance(negation_operator_arg, UnaryStructure)
                nested_leaf = negative_index_to_tree_plan_node[negative_index]
                if isinstance(nested_leaf, TreePlanUnaryNode):
                    current_leaf = nested_leaf
                    current_leaf.index = negative_index
                    if isinstance(current_leaf.child, TreePlanNestedNode):
                        current_leaf.child.nested_event_index = negative_index
                else:
                    nested_node_cost = negative_index_to_tree_plan_cost[negative_index]
                    nested_node_args = negation_operator_arg.args if isinstance(negation_operator_arg, CompositeStructure) \
                        else [negation_operator_arg.arg]
                    current_leaf = TreePlanNestedNode(negative_index, nested_leaf, nested_node_args, nested_node_cost)
            else:
                temp_leaf_index = len(pattern.positive_structure.args) + i
                current_leaf = TreePlanLeafNode(temp_leaf_index, negation_operator_arg.type, negation_operator_arg.name)
            positive_tree_plan = NegationAlgorithm._instantiate_negative_node(pattern, positive_tree_plan,
                                                                              current_leaf, is_unbounded)
        return positive_tree_plan
