"""
This file contains the implementations of algorithms constructing a left-deep tree-based evaluation mechanism.
"""
from typing import Dict

from base.Pattern import Pattern
from plan.TreePlan import TreePlanLeafNode, TreePlan, TreePlanNode, TreePlanInternalNode, TreePlanUnaryNode, \
    TreePlanBinaryNode
from plan.multi.TreePlanMerger import TreePlanMerger


class ShareLeavesTreePlanMerger(TreePlanMerger):
    """
    A class for deep tree builders.
    """

    def __init__(self):
        self.trees_number_nodes_shared = 0

    def merge_tree_plans(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan):
        if isinstance(pattern_to_tree_plan_map, TreePlan) or len(pattern_to_tree_plan_map) <= 1:
            return pattern_to_tree_plan_map

        pattern_to_tree_plan_map_copy = pattern_to_tree_plan_map.copy()
        return self.__share_leaves(pattern_to_tree_plan_map_copy)

    def __share_leaves(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan):
        """ this function is the core implementation for the trivial_sharing_trees algorithm """
        self.trees_number_nodes_shared = 0
        first_pattern = list(pattern_to_tree_plan_map.keys())[0]

        leaves_dict = TreePlanMerger.get_pattern_leaves_dict(pattern_to_tree_plan_map)

        shared_leaves_dict = {}
        for leaf in leaves_dict[first_pattern]:
            shared_leaves_dict[leaf] = [first_pattern,
                                        TreePlanLeafNode(leaf.event_index, leaf.event_type, leaf.event_name)]
        for pattern in list(pattern_to_tree_plan_map)[1:]:
            curr_leaves = leaves_dict[pattern]
            for curr_leaf in curr_leaves.keys():
                leaf_tree_plan_node = TreePlanLeafNode(curr_leaf.event_index, curr_leaf.event_type,
                                                       curr_leaf.event_name)
                for leaf in shared_leaves_dict.keys():
                    leaf_pattern, _ = shared_leaves_dict[leaf]
                    curr_leaf_pattern = pattern
                    first_event = leaves_dict[curr_leaf_pattern][curr_leaf]
                    second_event = leaves_dict[leaf_pattern][leaf]
                    # get the condition for every leaf
                    first_condition, second_condition = TreePlanMerger.get_condition_from_pattern_in_sub_tree(curr_leaf,
                                                                                                              curr_leaf_pattern,
                                                                                                              leaf,
                                                                                                              leaf_pattern,
                                                                                                              leaves_dict)
                    if first_condition is None or second_condition is None:
                        continue
                    if first_condition == second_condition and first_event.type == second_event.type:
                        self.trees_number_nodes_shared += 1
                        _, leaf_tree_plan_node = shared_leaves_dict[leaf]
                        break
                shared_leaves_dict[curr_leaf] = [pattern, leaf_tree_plan_node]
        return ShareLeavesTreePlanMerger._tree_plans_update_leaves(pattern_to_tree_plan_map,
                                                                   shared_leaves_dict)  # the unified tree


    @staticmethod
    def _tree_plans_update_leaves(pattern_to_tree_plan_map: Dict[Pattern, TreePlan], shared_leaves_dict):
        """at this function we share same leaves in different patterns to share the same root ,
        that says one instance of each unique leaf is created ,
        (if a leaf exists in two trees , the leaf will have two parents)
        """
        for pattern, tree_plan in pattern_to_tree_plan_map.items():
            updated_tree_plan_root = ShareLeavesTreePlanMerger._single_tree_plan_update_leaves(tree_plan.root,
                                                                                               shared_leaves_dict)
            pattern_to_tree_plan_map[pattern].root = updated_tree_plan_root
        return pattern_to_tree_plan_map

    @staticmethod
    def _single_tree_plan_update_leaves(tree_plan_root_node: TreePlanNode, shared_leaves_dict):

        """
        here we updated all the tree_plan_leaf in tree_plan_root to his matched one in shared_leaves_dict
        """
        if tree_plan_root_node is None:
            return tree_plan_root_node

        if type(tree_plan_root_node) == TreePlanLeafNode:
            pattern, updated_tree_plan_leaf = shared_leaves_dict[tree_plan_root_node]
            tree_plan_root_node = updated_tree_plan_leaf
            return tree_plan_root_node

        assert issubclass(type(tree_plan_root_node), TreePlanInternalNode)

        if type(tree_plan_root_node) == TreePlanUnaryNode:
            updated_child = ShareLeavesTreePlanMerger._single_tree_plan_update_leaves(tree_plan_root_node.child,
                                                                                      shared_leaves_dict)
            tree_plan_root_node.child = updated_child
            return tree_plan_root_node

        if type(tree_plan_root_node) == TreePlanBinaryNode:
            updated_left_child = ShareLeavesTreePlanMerger._single_tree_plan_update_leaves(tree_plan_root_node.left_child,
                                                                                           shared_leaves_dict)
            updated_right_child = ShareLeavesTreePlanMerger._single_tree_plan_update_leaves(tree_plan_root_node.right_child,
                                                                                            shared_leaves_dict)
            tree_plan_root_node.left_child = updated_left_child
            tree_plan_root_node.right_child = updated_right_child
            return tree_plan_root_node

        else:
            raise Exception("Unsupported Node type")


