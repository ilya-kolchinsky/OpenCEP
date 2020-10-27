from typing import List, Dict
from base.Pattern import Pattern
from plan.multi.MultiPatternEvaluationParameters import *
from tree.Tree import Tree
from tree.Node import Node
from tree.UnaryNode import UnaryNode
from tree.BinaryNode import BinaryNode
from tree.NegationNode import NegationNode
from tree.PatternMatchStorage import TreeStorageParameters
from plan.TreePlan import TreePlan


class MultiPatternTree:
    """
    Represents a multi-pattern evaluation tree.
    """
    def __init__(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                 storage_params: TreeStorageParameters,
                 multi_pattern_eval_params: MultiPatternEvaluationParameters):
        self.__pattern_to_output_node_dict = {}
        self.__output_nodes = self.__construct_multi_pattern_tree(pattern_to_tree_plan_map, storage_params,
                                                                  multi_pattern_eval_params.approach)
        self.__id_to_pattern_map = {pattern.id: pattern for pattern in pattern_to_tree_plan_map.keys()}

    def __construct_multi_pattern_tree(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                                       storage_params: TreeStorageParameters,
                                       multi_pattern_eval_approach: MultiPatternEvaluationApproaches):
        """
        Constructing a multi-pattern evaluation tree according to the approach.
        """
        if multi_pattern_eval_approach == MultiPatternEvaluationApproaches.TRIVIAL_SHARING_LEAVES:
            return self.__construct_trivial_tree(pattern_to_tree_plan_map, storage_params)
        if multi_pattern_eval_approach == MultiPatternEvaluationApproaches.SUBTREES_UNION:
            return self.__construct_subtrees_union_tree(pattern_to_tree_plan_map, storage_params)
        raise Exception("Unknown multi-pattern evaluation approach: %s" % (multi_pattern_eval_approach,))

    def __construct_trees_for_patterns(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                                       storage_params: TreeStorageParameters):
        """
        Creates a list of tree objects corresponding to the specified tree plans.
        """
        i = 1  # pattern IDs starts from 1
        trees = []
        for pattern, plan in pattern_to_tree_plan_map.items():
            trees.append(Tree(plan, pattern, storage_params, i))
            i += 1
        return trees

    def __construct_trivial_tree(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                                 storage_params: TreeStorageParameters):
        """
        This method gets patterns, builds a single-pattern tree to each one of them,
        and merges equivalent leaves from different trees.
        We are not sharing leaves from the same tree.
        We are assuming that each pattern appears only once in patterns (which is a legitimate assumption).
        """
        trees = self.__construct_trees_for_patterns(pattern_to_tree_plan_map, storage_params)
        self.__output_nodes = []
        # a map between a leaf and the number of equal leaves that were
        # shared to this leaf in the current iteration
        leaves_to_counter_dict = {}

        # a map between a leaf and a list of its equivalent leaves
        leaves_dict = {}

        for tree in trees:
            curr_leaves = tree.get_leaves()
            curr_root = tree.get_root()
            pattern_id = list(curr_leaves[0].get_pattern_ids())[0]
            self.__pattern_to_output_node_dict[pattern_id] = curr_root
            self.__output_nodes.append(curr_root)
            for leaf in curr_leaves:
                for dict_leaf in leaves_dict:
                    if leaf.is_equivalent(dict_leaf):
                        if leaves_to_counter_dict[dict_leaf] == len(leaves_dict[dict_leaf]):
                            # there are no free leaves to share
                            leaves_dict[dict_leaf].append(leaf)
                        else:
                            # the index of the first leaf that has not been shared yet
                            index = leaves_to_counter_dict[dict_leaf]
                            leaf_to_merge_into = leaves_dict[dict_leaf][index]
                            self.__merge_nodes(leaf_to_merge_into, leaf)
                        leaves_to_counter_dict[dict_leaf] += 1

                    else:
                        leaves_to_counter_dict[leaf] = 1
                        leaves_dict[leaf] = [leaf]

            leaves_to_counter_dict = {key: 0 for key in leaves_to_counter_dict}
        return self.__output_nodes

    def __construct_subtrees_union_tree(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                                        storage_params: TreeStorageParameters):
        """
        This method gets patterns, builds a single-pattern tree to each one of them,
        and merges equivalent subtrees from different trees.
        We are sharing the maximal subtree that exists in the tree.
        We are assuming that each pattern appears only once in patterns (which is a legitimate assumption).
        """
        trees = self.__construct_trees_for_patterns(pattern_to_tree_plan_map, storage_params)
        self.__output_nodes = []
        for i in range(len(pattern_to_tree_plan_map)):
            node = trees[i].get_root()
            pattern_id = i+1
            self.__output_nodes.append(node)
            self.__pattern_to_output_node_dict[pattern_id] = node
            for output_node in self.__output_nodes:
                if output_node == node:
                    break
                if self.__try_to_share_and_merge_nodes(output_node, node):
                    break
        return self.__output_nodes

    def __try_to_share_and_merge_nodes(self, root: Node, node: Node):
        """
        This method is trying to share the node (and its subtree) and the tree of root.
        If the root and node are not equivalent, trying to share the children of node and root.
        """
        if self.__find_and_merge_node_into_subtree(root, node):
            return True
        if isinstance(node, BinaryNode):
            left_merge = self.__try_to_share_and_merge_nodes(root, node.get_left_subtree())
            if left_merge:
                return True
            return self.__try_to_share_and_merge_nodes(root, node.get_right_subtree())
        if isinstance(node, UnaryNode):
            return self.__try_to_share_and_merge_nodes(root, node.get_child())
        return False

    def __find_and_merge_node_into_subtree(self, root: Node, node: Node):
        """
        This method is trying to find node in the subtree of root (or an equivalent node).
        If such a node is found, it merges the equivalent nodes.
        """
        if root.is_equivalent(node):
            self.__merge_nodes(root, node)
            return True
        elif isinstance(root, BinaryNode):
            return self.__find_and_merge_node_into_subtree(root.get_left_subtree(), node) or \
                   self.__find_and_merge_node_into_subtree(root.get_right_subtree(), node)
        elif isinstance(root, UnaryNode):
            return self.__find_and_merge_node_into_subtree(root.get_child(), node)
        return False

    def __merge_nodes(self, node: Node, other: Node):
        """
        Merge two nodes, and update all the required information
        """
        # merges other into node
        if node.get_sliding_window() < other.get_sliding_window():
            node.propagate_sliding_window(other.get_sliding_window())
        node.add_pattern_ids(other.get_pattern_ids())
        other_parents = other.get_parents()
        if other_parents is not None:
            for parent in other_parents:
                if isinstance(parent, UnaryNode):
                    parent.replace_subtree(node)
                elif isinstance(parent, BinaryNode):
                    parent.replace_subtree(other, node)
        else:
            # other is an output node in it's tree. the new output node of the old_tree is node
            if not node.is_output_node():
                node.set_is_output_node(True)
                self.__output_nodes.append(node)
                # other is already in self.__output_nodes, therefore we need to remove it
            self.__output_nodes.remove(other)
            other_id = list(other.get_pattern_ids())[0]
            self.__pattern_to_output_node_dict[other_id] = node

    def get_leaves(self):
        """
        Returns all leaves in this multi-pattern-tree.
        """
        leaves = set()
        for output_node in self.__output_nodes:
            leaves |= set(output_node.get_leaves())
        return leaves

    def get_matches(self):
        """
        Returns the matches from all of the output nodes.
        """
        matches = []
        for output_node in self.__output_nodes:
            while output_node.has_unreported_matches():
                match = output_node.get_next_unreported_match()
                pattern_ids = output_node.get_pattern_ids()
                for pattern_id in pattern_ids:
                    if self.__pattern_to_output_node_dict[pattern_id] != output_node:
                        # the current output node is an internal node in pattern #idx, but not it's root.
                        # we don't need to check if there are any matches for this pattern id.
                        continue
                    # check if timestamp is correct for this pattern id.
                    # the pattern indices start from 1.
                    if match.last_timestamp - match.first_timestamp <= self.__id_to_pattern_map[pattern_id].window:
                        match.add_pattern_id(pattern_id)
                matches.append(match)
        return matches

    def get_last_matches(self):
        """
        This method is similar to the method- get_last_matches in a Tree.
        """
        for output_node in self.__output_nodes:
            if not isinstance(output_node, NegationNode):
                continue
                # this is the node that contains the pending matches
            first_unbounded_negative_node = output_node.get_first_unbounded_negative_node()
            if first_unbounded_negative_node is None:
                continue
            first_unbounded_negative_node.flush_pending_matches()
            # the pending matches were released and have hopefully reached the roots

        return self.get_matches()
