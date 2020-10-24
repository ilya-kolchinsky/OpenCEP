from typing import List
from base.Pattern import Pattern
from plan.MultiPatternEvaluationParameters import *
from tree.Tree import Tree
from tree.Node import Node
from tree.UnaryNode import UnaryNode
from tree.BinaryNode import BinaryNode
from tree.NegationNode import NegationNode
from tree.PatternMatchStorage import TreeStorageParameters
from plan.TreePlan import TreePlan


class MultiPatternTree:
    """
    Represents a multi- pattern evaluation tree. Using the single- pattern tree builder.
    """

    def __init__(self, tree_plans, patterns: List[Pattern], storage_params: TreeStorageParameters,
                 multi_pattern_eval_params: MultiPatternEvaluationParameters):
        self.__pattern_to_root_dict = {}
        multi_pattern_eval_approach = multi_pattern_eval_params.approach
        self.__roots = self.__construct_multi_pattern_tree(tree_plans, patterns, storage_params,
                                                           multi_pattern_eval_approach)
        self.__patterns = patterns

        pass

    def __construct_multi_pattern_tree(self, tree_plans: List[TreePlan], patterns: List[Pattern], storage_params: TreeStorageParameters,
                                       multi_pattern_eval_approach: MultiPatternEvaluationApproach):

        if multi_pattern_eval_approach == MultiPatternEvaluationApproach.TRIVIAL_SHARING_LEAVES:
            return self.__construct_trivial_tree(tree_plans, patterns, storage_params)
        if multi_pattern_eval_approach == MultiPatternEvaluationApproach.SUBTREES_UNION:
            return self.__construct_subtrees_union_tree(tree_plans, patterns, storage_params)

    """
    This method gets patterns, builds a single-pattern tree to each one of them,
    and merges equivalent leaves from different trees.
    We are not sharing leaves from the same tree.
    We are assuming that each pattern only appears once in patterns (which is a legitimate assumption)
    """
    def __construct_trivial_tree(self, tree_plans, patterns: List[Pattern],
                                 storage_params: TreeStorageParameters):

        trees = [Tree(tree_plans[patterns[i]], patterns[i], storage_params, i+1) for i in range(len(patterns))]
        self.__roots = []
        # a map between a leaf and the number of equal leaves that were
        # shared to this leaf in the current iteration
        leaves_to_counter_dict = {}

        # a map between a leaf and a list of its equivalent leaves
        leaves_dict = {}

        for tree in trees:
            curr_leaves = tree.get_leaves()
            curr_root = tree.get_root()
            pattern_id = list(curr_leaves[0].get_pattern_ids())[0]
            self.__pattern_to_root_dict[pattern_id] = curr_root
            self.__roots.append(curr_root)
            for leaf in curr_leaves:
                for dict_leaf in leaves_dict:
                    if leaf.is_equal(dict_leaf):
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
        return self.__roots

    """
        This method gets patterns, builds a single-pattern tree to each one of them,
        and merges equivalent subtrees from different trees.
        We are sharing the maximal subtree that exists in the tree.
        We are assuming that each pattern only appears once in patterns (which is a legitimate assumption)
        """
    def __construct_subtrees_union_tree(self, tree_plans: List[TreePlan], patterns: List[Pattern],
                                        storage_params: TreeStorageParameters):
        trees = [Tree(tree_plans[patterns[i]], patterns[i], storage_params, i+1) for i in range(len(patterns))]
        self.__roots = []
        for i in range(len(patterns)):
            node = trees[i].get_root()
            pattern_id = i+1
            self.__roots.append(node)
            self.__pattern_to_root_dict[pattern_id] = node
            for root in self.__roots:
                if root == node:
                    break
                if self.__try_and_merge(root, node):
                    break
        return self.__roots

    """
    This method is trying to share the node (and its subtree) and the tree of root.
    If the root and node is not equivalent, trying to share the children of node and root.
    """
    def __try_and_merge(self, root, node):
        if self.__find_and_merge_node_into_subtree(root, node):
            return True
        if isinstance(node, BinaryNode):
            left_merge = self.__try_and_merge(root, node.get_left_subtree())
            if left_merge:
                return True
            return self.__try_and_merge(root, node.get_right_subtree())
        if isinstance(node, UnaryNode):
            return self.__try_and_merge(root, node.get_child())
        return False

    """
    This method is trying to find node in the subtree of root (or an equivalent node).
    If such a node is found, it merges the equivalent nodes.
    """
    def __find_and_merge_node_into_subtree(self, root: Node, node: Node):
        if root.is_equal(node):
            self.__merge_nodes(root, node)
            return True
        elif isinstance(root, BinaryNode):
            return self.__find_and_merge_node_into_subtree(root.get_left_subtree(), node) or \
                   self.__find_and_merge_node_into_subtree(root.get_right_subtree(), node)
        elif isinstance(root, UnaryNode):
            return self.__find_and_merge_node_into_subtree(root.get_child(), node)
        return False

    """
    Merge two nodes, and update all the required information
    """
    def __merge_nodes(self, node: Node, other: Node):
        # merges other into node
        if node.get_sliding_window() < other.get_sliding_window():
            node.update_sliding_window(other.get_sliding_window())
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
                self.__roots.append(node)
                # other is already in self.__roots, therefore we need to remove it
            self.__roots.remove(other)
            other_id = list(other.get_pattern_ids())[0]
            self.__pattern_to_root_dict[other_id] = node

    def get_leaves(self):
        leaves = set()
        for root in self.__roots:
            leaves |= set(root.get_leaves())
        return leaves

    def get_matches(self):
        matches = []
        for root in self.__roots:
            while root.has_unreported_matches():
                match = root.get_last_unreported_match()
                pattern_idx = root.get_pattern_ids()
                for idx in pattern_idx:
                    if self.__pattern_to_root_dict[idx] != root:
                        # the current root is an internal node in pattern #idx, but not it's root.
                        # we don't need to check if there are any matches for this pattern id
                        continue
                    # check the if timestamp is correct for this pattern id.
                    # the pattern indices start from 1.
                    if match.last_timestamp - match.first_timestamp <= self.__patterns[idx-1].window:
                        match.add_pattern_id(idx)
                matches.append(match)
        return matches

    def get_last_matches(self):
        for root in self.__roots:
            if not isinstance(root, NegationNode):
                continue
                # this is the node that contains the pending matches
            first_unbounded_negative_node = root.get_first_unbounded_negative_node()
            if first_unbounded_negative_node is None:
                continue
            first_unbounded_negative_node.flush_pending_matches()
            # the pending matches were released and have hopefully reached the roots

        return self.get_matches()


