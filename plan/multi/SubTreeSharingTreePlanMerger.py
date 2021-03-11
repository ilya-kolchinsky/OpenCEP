"""
This file contains the implementations of algorithms constructing a left-deep tree-based evaluation mechanism.
"""
from typing import Dict

from base.Pattern import Pattern
from plan.TreePlan import TreePlanLeafNode, TreePlan, TreePlanNode, TreePlanUnaryNode, TreePlanBinaryNode
from plan.TreePlanBuilder import TreePlanBuilder
from plan.multi.TreePlanMerger import TreePlanMerger


class SubTreeSharingTreePlanMerger(TreePlanMerger):
    """
    A class for deep tree builders.
    """

    def __init__(self):
        self.trees_number_nodes_shared = 0

    def merge_tree_plans(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan):
        if isinstance(pattern_to_tree_plan_map, TreePlan) or len(pattern_to_tree_plan_map) <= 1:
            return pattern_to_tree_plan_map

        pattern_to_tree_plan_map_copy = pattern_to_tree_plan_map.copy()
        return self.__construct_subtrees_union_tree_plan(pattern_to_tree_plan_map_copy)

    def __construct_subtrees_union_tree_plan(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan):
        """
        This method gets patterns, builds a single-pattern tree to each one of them,
        and merges equivalent subtrees from different trees.
        We are sharing the maximal subtree that exists in the tree.
        We are assuming that each pattern appears only once in patterns (which is a legitimate assumption).
        """
        self.trees_number_nodes_shared = 0

        leaves_dict = TreePlanMerger.get_pattern_leaves_dict(pattern_to_tree_plan_map)

        self.leaves_dict = leaves_dict

        output_nodes = {}
        for i, pattern in enumerate(pattern_to_tree_plan_map):
            tree_plan = pattern_to_tree_plan_map[pattern]
            current_root = tree_plan.root
            output_nodes[current_root] = pattern
            for root in output_nodes:
                if root == current_root:
                    break
                new_root, number_shared = self.__try_to_share_and_merge_nodes(current_root, pattern, root,
                                                                              output_nodes[root])
                if number_shared:
                    self.trees_number_nodes_shared += number_shared
                    pattern_to_tree_plan_map[pattern].root = new_root
                    break

        return pattern_to_tree_plan_map

    def __try_to_share_and_merge_nodes(self, root, root_pattern: Pattern, node, node_pattern: Pattern):
        """
            This method is trying to share the node (and its subtree) and the tree of root.
            If the root and node are not equivalent, trying to share the children of node and root.
        """
        if root is None or node is None:
            return root, 0

        merged_node, number_merged = self.__find_and_merge_node_into_subtree(root, root_pattern, node, node_pattern)

        if number_merged:
            return merged_node, number_merged

        if isinstance(node, TreePlanBinaryNode):
            return self.__try_to_share_and_merge_nodes_binary(root, root_pattern, node, node_pattern)

        if isinstance(node, TreePlanUnaryNode):
            updated_root, number_merged = self.__try_to_share_and_merge_nodes(root, root_pattern, node.child,
                                                                              node_pattern)
            if number_merged:
                return updated_root, number_merged

        return root, 0

    def __try_to_share_and_merge_nodes_binary(self, root, root_pattern: Pattern, node, node_pattern: Pattern):

        # tried to find matching subtrees in calling the left subtree of node
        left_side_new_root, number_merged_left = self.__try_to_share_and_merge_nodes(root, root_pattern,
                                                                                     node.left_child, node_pattern)
        if number_merged_left > 0:
            right_side_new_root, number_merged_right = self.__try_to_share_and_merge_nodes(root, root_pattern,
                                                                                           node.right_child,
                                                                                           node_pattern)
            if number_merged_right > 0:
                return right_side_new_root, number_merged_left + number_merged_right
            return left_side_new_root, number_merged_left + number_merged_right

        right_side_new_root, number_merged_right = self.__try_to_share_and_merge_nodes(root, root_pattern,
                                                                                       node.right_child, node_pattern)
        if number_merged_right > 0:
            return right_side_new_root, number_merged_right
        return root, 0

    def __find_and_merge_node_into_subtree(self, root: TreePlanNode, root_pattern: Pattern, node: TreePlanNode,
                                           node_pattern: Pattern):
        """
        This method is trying to find node in the subtree of root (or an equivalent node).
        If such a node is found, it merges the equivalent nodes.
        """
        if TreePlanBuilder.is_equivalent(root, root_pattern, node, node_pattern, self.leaves_dict):
            if type(root) == TreePlanLeafNode:
                events = self.leaves_dict[node_pattern][node]
                self.leaves_dict[node_pattern].pop(node)
                self.leaves_dict[node_pattern][root] = events
            return node, SubTreeSharingTreePlanMerger._sub_tree_size(node)

        elif isinstance(root, TreePlanBinaryNode):
            left_find_and_merge, number_of_merged_left = self.__find_and_merge_node_into_subtree(root.left_child,
                                                                                                 root_pattern, node,
                                                                                                 node_pattern)
            right_find_and_merge, number_of_merged_right = self.__find_and_merge_node_into_subtree(root.right_child,
                                                                                                   root_pattern, node,
                                                                                                   node_pattern)

            if number_of_merged_left:
                root.left_child = left_find_and_merge

            if number_of_merged_right:
                root.right_child = right_find_and_merge

            return root, number_of_merged_left + number_of_merged_right

        elif isinstance(root, TreePlanUnaryNode):
            child_find_and_merge, is_child_merged = self.__find_and_merge_node_into_subtree(root.child, root_pattern,
                                                                                            node, node_pattern)

            if is_child_merged:
                root.child = child_find_and_merge

            return root, is_child_merged

        return root, 0

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
            return 1 + TreePlanMerger._sub_tree_size(root.left_child) + TreePlanMerger._sub_tree_size(
                root.right_child)

        if isinstance(root, TreePlanUnaryNode):
            return 1 + TreePlanMerger._sub_tree_size(root.child)

        raise Exception("Unsupported tree plan node type")
