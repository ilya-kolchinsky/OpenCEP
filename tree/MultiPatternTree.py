from typing import List
from queue import Queue
from base.Pattern import Pattern
from plan.MultiPatternEvaluationApproach import MultiPatternEvaluationApproach
from tree.Tree import Tree
from tree.Node import PrimitiveEventDefinition, Node
from tree.UnaryNode import UnaryNode
from tree.BinaryNode import BinaryNode
from tree.NegationNode import NegationNode
from tree.LeafNode import LeafNode
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
                    if curr_parents:
                        for parent in curr_parents:
                            our_leaf.add_to_dict(parent, PrimitiveEventDefinition(leaf.get_event_type(), leaf.get_event_name(), leaf.get_leaf_index()))
                            our_leaf.add_to_parent_to_unhandled_queue_dict(parent, Queue())
                            if isinstance(parent, UnaryNode):
                                parent.replace_subtree(our_leaf)
                            elif isinstance(parent, BinaryNode):
                                parent.replace_subtree(leaf, our_leaf)
                    # this means that we are in a root
                    else:
                        pattern_id = list(leaf.get_pattern_id())[0]
                        self.__pattern_to_root_dict[pattern_id] = our_leaf
                        curr_root = []
                        our_leaf.set_is_root(True)

                    leaves_to_counter_dict[dict_leaf] += 1

                flag = 0
            leaves_to_counter_dict = {key: 0 for key in leaves_to_counter_dict}
            roots += curr_root
        return roots

    def __construct_subtrees_union_tree(self, tree_plans: List[TreePlan], patterns: List[Pattern],
                                        storage_params: TreeStorageParameters):
        trees = [Tree(tree_plans[i], patterns[i], storage_params, i+1) for i in range(len(patterns))]
        self.__roots = []
        for i in range(len(patterns)):
            node = trees[i].get_root()
            index = i+1
            self.__roots.append(node)
            self.__pattern_to_root_dict[index] = node
            for root in self.__roots:
                if root == node:
                    break
                if self.try_and_merge(root, node, index):
                    break
        return self.__roots

    def try_and_merge(self, root, node, index):
        if self.find_and_merge_node_into_subtree(root, node, index):
            return True
        if isinstance(node, BinaryNode):
            left_merge = self.try_and_merge(root, node.get_left_subtree(), index)
            right_merge = self.try_and_merge(root, node.get_right_subtree(), index)
            return left_merge or right_merge
        if isinstance(node, UnaryNode):
            return self.try_and_merge(root, node.get_child(), index)
        return False

    def find_and_merge_node_into_subtree(self, root: Node, node: Node, index):
        # in this check we make sure that we are not checking shared nodes more than once
        # if len(root.get_pattern_id()) > 1 and min(root.get_pattern_id()) < index:
        #    return False
        if root.is_equal(node):
            self.merge_nodes(root, node)
            return True
        elif isinstance(root, BinaryNode):
            return self.find_and_merge_node_into_subtree(root.get_left_subtree(), node, index) or \
                   self.find_and_merge_node_into_subtree(root.get_right_subtree(), node, index)
        elif isinstance(root, UnaryNode):
            return self.find_and_merge_node_into_subtree(root.get_child(), node, index)
        return False

    def merge_nodes(self, node: Node, other: Node):
        #merges other into node
        if node.get_sliding_window() < other.get_sliding_window():
            node.update_sliding_window(other.get_sliding_window())
        node.add_pattern_id(other.get_pattern_id())
        other_parents = other.get_parents()
        if other_parents is not None:
            for parent in other_parents:
                # assuming each node is either LeafNode or InternalNode and therefore has a get_event_definitions() method
                if isinstance(other, LeafNode):
                    event_defs = other.get_event_definitions()[0]
                else:
                    event_defs = other.get_event_definitions()

                node.add_to_dict(parent,event_defs)
                node.add_to_parent_to_unhandled_queue_dict(parent, Queue())
                if isinstance(parent, UnaryNode):
                    parent.replace_subtree(node)
                elif isinstance(parent, BinaryNode):
                    parent.replace_subtree(other, node)
        else:
            # other is a root in it's tree. the new root of the old_tree is node
            if not node.is_root():
                node.set_is_root(True)
                self.__roots.append(node)
                #other is already in self.__roots, therefore we need to remove it
            self.__roots.remove(other)
            other_id = list(other.get_pattern_id())[0]
            self.__pattern_to_root_dict[other_id] = node

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



