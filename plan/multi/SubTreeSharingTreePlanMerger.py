"""
This file contains the implementations of algorithms constructing a left-deep tree-based evaluation mechanism.
"""
from copy import deepcopy
from typing import Dict, List

from base.Pattern import Pattern
from base.PatternStructure import PrimitiveEventStructure
from condition.Condition import Variable
from plan.TreePlan import TreePlanLeafNode, TreePlan, TreePlanNode, TreePlanUnaryNode, TreePlanBinaryNode, \
    TreePlanInternalNode
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


    @staticmethod
    def replace_name_by_type_condition(first_condition, second_condition, first_pattern_events: List[PrimitiveEventStructure],
                                       second_pattern_events: List[PrimitiveEventStructure]):

        # replace every event's name in condition by his type in order to compare between condition.
        first_events_name_type = {event.name: event.type for event in first_pattern_events}
        second_events_name_type = {event.name: event.type for event in second_pattern_events}

        for e in first_condition.get_conditions_list():
            if type(e.left_term_repr) == Variable:
                e.left_term_repr.name = first_events_name_type[e.left_term_repr.name]
            if type(e.right_term_repr) == Variable:
                e.right_term_repr.name = first_events_name_type[e.right_term_repr.name]
        for e in second_condition.get_conditions_list():
            if type(e.left_term_repr) == Variable:
                e.left_term_repr.name = second_events_name_type[e.left_term_repr.name]
            if type(e.right_term_repr) == Variable:
                e.right_term_repr.name = second_events_name_type[e.right_term_repr.name]

        return first_condition, second_condition

    @staticmethod
    def extract_pattern_condition(plan_node, pattern, leaves_dict):
        leaves_in_plan_node = plan_node.get_leaves()
        if leaves_in_plan_node is None:
            return None

        pattern_leaves, pattern_events = list(zip(*list(leaves_dict.get(pattern).items())))

        event_indexes = list(map(lambda e: e.event_index, leaves_in_plan_node))
        plan_node_events = list(
            filter(lambda i: pattern_leaves[i].event_index in event_indexes, range(len(pattern_leaves))))

        names = {pattern_events[event_index].name for event_index in plan_node_events}
        return names, pattern_events, event_indexes

    @staticmethod
    def get_condition_from_pattern_in_sub_tree(first_plan_node: TreePlanNode, first_pattern: Pattern, second_plan_node: TreePlanNode,
                                               second_pattern: Pattern,
                                               leaves_dict):

        first_names, first_pattern_events, _ = TreePlanBuilder.extract_pattern_condition(first_plan_node, first_pattern, leaves_dict)
        second_names, second_pattern_events, _ = TreePlanBuilder.extract_pattern_condition(second_plan_node, second_pattern, leaves_dict)
        first_condition = deepcopy(first_pattern.condition.get_condition_of(first_names, get_kleene_closure_conditions=False,
                                                                       consume_returned_conditions=False))
        second_condition = deepcopy(second_pattern.condition.get_condition_of(second_names, get_kleene_closure_conditions=False,
                                                                        consume_returned_conditions=False))

        if first_condition == second_condition:
            return first_condition, second_condition
        return TreePlanBuilder.replace_name_by_type_condition(first_condition, second_condition, first_pattern_events,
                                                              second_pattern_events)

    @staticmethod
    def is_equivalent(first_plan_node: TreePlanNode, first_pattern: Pattern, second_plan_node: TreePlanNode, second_pattern: Pattern,
                      leaves_dict: Dict[Pattern, Dict[TreePlanNode, PrimitiveEventStructure]]):

        """
        find if two subtree_plans are euivalent and check that by recursion on left subtree_plans and right subtree_plans
        the way this function works is comparing one node in pattern1 with its corresponding node in pattern2 , in addition to comparing
        the hierarchy we compare the conditions too, if we counter two nodes with different condition set or different subtrees hierarchy
        , we return false
        """
        if type(first_plan_node) != type(second_plan_node) or first_plan_node is None or second_plan_node is None:
            return False
        # we have to extract both condition lists since it's not possible to implement this function using __eq__
        # hierarchy because the input is not and instance of this class .
        first_condition, second_condition = TreePlanBuilder.get_condition_from_pattern_in_sub_tree(first_plan_node, first_pattern,
                                                                                                   second_plan_node, second_pattern,
                                                                                                   leaves_dict)

        if first_condition is None or second_condition is None or first_condition != second_condition:
            return False

        nodes_type = type(first_plan_node)

        if issubclass(nodes_type, TreePlanInternalNode):
            if first_plan_node.operator != second_plan_node.operator:
                return False
            if nodes_type == TreePlanUnaryNode:
                return TreePlanBuilder.is_equivalent(first_plan_node.child, first_pattern, second_plan_node.child, second_pattern,
                                                     leaves_dict)

            if nodes_type == TreePlanBinaryNode:
                return TreePlanBuilder.is_equivalent(first_plan_node.left_child, first_pattern, second_plan_node.left_child,
                                                     second_pattern, leaves_dict) \
                       and TreePlanBuilder.is_equivalent(first_plan_node.right_child, first_pattern, second_plan_node.right_child,
                                                         second_pattern, leaves_dict)

        if nodes_type == TreePlanLeafNode:
            first_event = leaves_dict.get(first_pattern).get(first_plan_node)
            second_event = leaves_dict.get(second_pattern).get(second_plan_node)
            if first_event and second_event:
                return first_event.type == second_event.type

        return False
