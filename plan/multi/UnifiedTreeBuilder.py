"""
This file contains the implementations of algorithms constructing a left-deep tree-based evaluation mechanism.
"""
from typing import Dict

from base.Pattern import Pattern
from plan.TreePlan import TreePlanLeafNode, TreePlan, TreePlanUnaryNode, TreePlanBinaryNode


class UnifiedTreeBuilder:
    """
    A class for deep tree builders.
    """
    def unite_tree_plans(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan):
        """
        Transforms a raw data object representing a single event into a dictionary of objects, each corresponding
        to a single event attribute.
        """
        raise NotImplementedError()

    @staticmethod
    def _sub_tree_size(root):
        """
        get the size of subtree
        """
        if root is None:
            return 0

        if isinstance(root, TreePlanLeafNode):
            return 1

        if isinstance(root, TreePlanBinaryNode):
            return 1 + UnifiedTreeBuilder._sub_tree_size(root.left_child) + UnifiedTreeBuilder._sub_tree_size(
                root.right_child)

        if isinstance(root, TreePlanUnaryNode):
            return 1 + UnifiedTreeBuilder._sub_tree_size(root.child)

        raise Exception("Unsupported tree plan node type")

    @staticmethod
    def get_pattern_leaves_dict(pattern_to_tree_plan_map: Dict[Pattern, TreePlan]):
        """
                A helper method for return a [pattern -> [ [treePlanNode, Event] ...]] mapper.
        """
        leaves_dict = {}
        for i, pattern in enumerate(pattern_to_tree_plan_map):
            tree_plan_leaves_pattern = pattern_to_tree_plan_map[pattern].root.get_leaves()
            pattern_args = pattern.positive_structure.get_args()
            pattern_event_size = len(pattern_args)
            leaves_dict[pattern] = {tree_plan_leaves_pattern[i]: pattern_args[tree_plan_leaves_pattern[i].event_index]
                                    for i in
                                    range(pattern_event_size)}
        return leaves_dict


