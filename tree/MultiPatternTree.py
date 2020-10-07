from typing import List
from queue import Queue
from base.Pattern import Pattern
from plan.MultiPatternEvaluationApproach import MultiPatternEvaluationApproach
from tree.Tree import Tree
from tree.Node import PrimitiveEventDefinition, Node
from tree.UnaryNode import UnaryNode
from tree.BinaryNode import BinaryNode
from tree.NegationNode import NegationNode
from tree.PatternMatchStorage import TreeStorageParameters
from plan.TreePlan import TreePlan


class MultiPatternTree:
    """
    Represents a multi- pattern evaluation tree. Using the single- pattern tree builder.
    """

    def __init__(self, tree_plans: List[TreePlan], patterns: List[Pattern], storage_params: TreeStorageParameters,
                 multi_pattern_eval_approach: MultiPatternEvaluationApproach):
        self.__pattern_to_root_dict = {}
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

    def __construct_trivial_tree(self, tree_plans: List[TreePlan], patterns: List[Pattern],
                                 storage_params: TreeStorageParameters):

        trees = [Tree(tree_plans[i], patterns[i], storage_params, i+1) for i in range(len(patterns))]
        roots = []
        leaves_to_counter_dict, leaves_dict = {}, {}
        flag = 0

        for tree in trees:
            curr_leaves = tree.get_leaves()
            curr_root = curr_leaves[0].get_roots()
            roots += curr_root
            pattern_id = list(curr_leaves[0].get_pattern_id())[0]
            self.__pattern_to_root_dict[pattern_id] = curr_root[0]
            for leaf in curr_leaves:
                for dict_leaf in leaves_dict:
                    if leaf.is_equal(dict_leaf):
                        flag = 1
                        break
                if flag == 0:
                    leaves_to_counter_dict[leaf] = 1
                    leaves_dict[leaf] = []
                    leaves_dict[leaf].append(leaf)

                elif leaves_to_counter_dict[dict_leaf] == len(leaves_dict[dict_leaf]):
                    leaves_dict[dict_leaf].append(leaf)
                    leaves_to_counter_dict[dict_leaf] += 1

                else:
                    index = leaves_to_counter_dict[dict_leaf]
                    our_leaf = leaves_dict[dict_leaf][index]
                    our_leaf.set_sliding_window(max(our_leaf.get_sliding_window(), leaf.get_sliding_window()))
                    our_leaf.add_pattern_id(leaf.get_pattern_id())
                    curr_parents = leaf.get_parents()
                    for parent in curr_parents:
                        our_leaf.add_to_dict(parent, PrimitiveEventDefinition(leaf.get_event_type(), leaf.get_event_name(), leaf.get_leaf_index()))
                        our_leaf.add_to_parent_to_unhandled_queue_dict(parent, Queue())
                        if isinstance(parent, UnaryNode):
                            parent.replace_subtree(our_leaf)
                        elif isinstance(parent, BinaryNode):
                            parent.replace_subtree(leaf, our_leaf)
                    leaves_to_counter_dict[dict_leaf] += 1

                flag = 0
            leaves_to_counter_dict = {key: 0 for key in leaves_to_counter_dict}
        return roots

    def __construct_subtrees_union_tree(self, tree_plans: List[TreePlan], patterns: List[Pattern],
                                        storage_params: TreeStorageParameters):
        trees = [Tree(tree_plans[i], patterns[i], storage_params, i+1) for i in range(len(patterns))]
        roots = trees[0].get_roots()
        for i in range(len(patterns)):
            node = trees[i].get_root()
            index = i+1
            for root in roots:

               while not find_and_merge_node_into_subtree(root, node, index):
                   if isinstance(node, BinaryNode):
                        node = node.get_left_subtree()
                        node = node.get_right_subtree()
                   if isinstance(node, UnaryNode):
                        node = node.get_child()

        return roots

    def get_leaves(self):
        leaves = set()
        for root in self.__roots:
            leaves |= set(root.get_leaves())
        pass
        return leaves

    def get_matches(self):
        matches = []
        for root in self.__roots:
            while root.has_partial_matches():
                match = root.consume_first_partial_match()
                pattern_idx = root.get_pattern_id()
                for idx in pattern_idx:
                    if self.__pattern_to_root_dict[idx] != root:
                        continue
                    # check the timestamp
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
"""
This method gets _root and _node, and tries to find a node which is
equivalent to _node in the subtree which _root is its root. If a node is found, it merges them.
"""
def find_and_merge_node_into_subtree(root: Node, node: Node, index):
    # in this check we make sure that we are not checking shared nodes more than once
    if min(root.get_pattern_id()) < index:
        return False
    if root.is_equal(node):
        root.unite(node)
        return True
    elif isinstance(root, BinaryNode):
        return find_and_merge_node_into_subtree(root.get_left_subtree(), node, index) or \
              find_and_merge_node_into_subtree(root.get_right_subtree(), node, index)
    elif isinstance(root, UnaryNode):
        return find_and_merge_node_into_subtree(root.get_child(), node, index)
    return False



