from typing import List, Dict

from base.Pattern import Pattern
from base.PatternStructure import NegationOperator, AndOperator, SeqOperator
from plan.TreePlan import TreePlanBinaryNode, TreePlanLeafNode, TreePlanNode, OperatorTypes, TreePlanUnaryNode, \
    TreePlanNegativeBinaryNode


class NegationAlgorithm:
    """
    The main class of the negation algorithms, contains the information and parameters needed in order to build the
    tree plan according to the chosen negation algorithm.
    """
    def handle_pattern_negation(self, pattern: Pattern, statistics: Dict, positive_tree_plan: TreePlanBinaryNode):
        """
        Augments the given tree plan, that only contains the positive portion of the given pattern, with the operators
        required for the negative part.
        """
        if pattern.negative_structure is None:
            return positive_tree_plan
        positive_indices, negative_indices = self.__calculate_positive_and_negative_indices(pattern)
        unbounded_negative_indices = []
        if pattern.full_structure.get_top_operator() == SeqOperator:
            # a sequence can contain bounded negative events - those followed by a positive event in the sequence order
            for index in negative_indices:
                if len([positive_index for positive_index in positive_indices if positive_index > index]) == 0:
                    unbounded_negative_indices.append(index)
        else:
            # no sequence operator - all negative events are unbounded
            unbounded_negative_indices = negative_indices

        full_tree_plan = self._add_negative_part(pattern, statistics, positive_tree_plan,
                                                 negative_indices, unbounded_negative_indices)

        self.__adjust_tree_plan_indices(full_tree_plan, pattern)
        return full_tree_plan

    def _add_negative_part(self, pattern: Pattern, statistics: Dict, positive_tree_plan: TreePlanBinaryNode,
                           all_negative_indices: List[int], unbounded_negative_indices: List[int]):
        """
        Actually adds negation operators to the given tree plan.
        """
        raise NotImplementedError()

    @staticmethod
    def __calculate_positive_and_negative_indices(pattern: Pattern):
        """
        Partitions the event indices in a given flat pattern into positive and negative ones (containing events under
        negation).
        """
        positive_indices = []
        negative_indices = []
        for index, arg in enumerate(pattern.full_structure.args):
            if type(arg) == NegationOperator:
                negative_indices.append(index)
            else:
                positive_indices.append(index)
        return positive_indices, negative_indices

    def __adjust_tree_plan_indices(self, tree_plan: TreePlanNode, pattern: Pattern):
        """
        Traverses the tree plan (including both positive and negative parts) and adjusts the leaf indices to reflect
        the negative structure.
        """
        positive_indices, negative_indices = self.__calculate_positive_and_negative_indices(pattern)
        self.__adjust_tree_plan_indices_aux(tree_plan, positive_indices, negative_indices)

    def __adjust_tree_plan_indices_aux(self, tree_plan: TreePlanNode,
                                       positive_indices: List[int], negative_indices: List[int]):
        """
        An auxiliary recursive function for adjusting the tree indices.
        """
        if isinstance(tree_plan, TreePlanLeafNode):
            old_index = tree_plan.event_index
            if old_index >= len(positive_indices):
                tree_plan.original_event_index = tree_plan.event_index = negative_indices[old_index - len(positive_indices)]
            else:
                tree_plan.original_event_index = tree_plan.event_index = positive_indices[old_index]
            return
        if isinstance(tree_plan, TreePlanUnaryNode):
            self.__adjust_tree_plan_indices_aux(tree_plan.child, positive_indices, negative_indices)
            return
        # isinstance(tree_plan, TreePlanBinaryNode)
        self.__adjust_tree_plan_indices_aux(tree_plan.left_child, positive_indices, negative_indices)
        self.__adjust_tree_plan_indices_aux(tree_plan.right_child, positive_indices, negative_indices)

    @staticmethod
    def _instantiate_negative_node(pattern: Pattern, positive_subtree: TreePlanNode, negative_subtree: TreePlanNode,
                                   is_unbounded: bool):
        """
        Creates a node representing a negation operator over the given positive and negative subtrees.
        """
        if isinstance(pattern.positive_structure, AndOperator):
            operator_type = OperatorTypes.NAND
        elif isinstance(pattern.positive_structure, SeqOperator):
            operator_type = OperatorTypes.NSEQ
        else:
            raise Exception("Unsupported operator for negation")
        return TreePlanNegativeBinaryNode(operator_type, positive_subtree, negative_subtree, is_unbounded)
