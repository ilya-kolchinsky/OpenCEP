from typing import List
from queue import Queue
from base.Pattern import Pattern
from plan.MultiPatternEvaluationApproach import MultiPatternEvaluationApproach
from tree.Tree import Tree
from tree.Node import PrimitiveEventDefinition
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
        self.__roots = self.__construct_multi_pattern_tree(tree_plans, patterns, storage_params,
                 multi_pattern_eval_approach)
        self.__pattern_to_root_dict = {i+1: tree_plans[i] for i in range(len(patterns))}


    def __construct_multi_pattern_tree(self, tree_plans: List[TreePlan], patterns: List[Pattern], storage_params: TreeStorageParameters,
                 multi_pattern_eval_approach: MultiPatternEvaluationApproach):

        if multi_pattern_eval_approach == MultiPatternEvaluationApproach.TRIVIAL_SHARING_LEAVES:
            return self.__construct_trivial_tree(tree_plans, patterns, storage_params)
        if multi_pattern_eval_approach == MultiPatternEvaluationApproach.SUBTREES_UNION:
            return self.__construct_subtrees_union_tree(tree_plans, patterns, storage_params)

    @staticmethod
    def __construct_trivial_tree(tree_plans: List[TreePlan], patterns: List[Pattern],
                                 storage_params: TreeStorageParameters):

        trees = [Tree(tree_plans[i], patterns[i], storage_params) for i in range(len(patterns))]
        roots = []
        leaves_to_counter_dict, leaves_dict = {}, {}
        flag = 0

        for tree in trees:
            curr_leaves = tree.get_leaves()
            roots += curr_leaves[0].get_roots()
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

    @staticmethod
    def __construct_subtrees_union_tree(tree_structures: List[tuple], patterns: List[Pattern],
                                        storage_params: TreeStorageParameters):
        pass

    @staticmethod
    def __construct_local_search_tree(tree_structures: List[tuple], patterns: List[Pattern],
                                      storage_params: TreeStorageParameters):
        pass

    def get_leaves(self):
        leaves = set()
        for root in self.__roots:
            leaves |= set(root.get_leaves())
        pass
        return leaves

    def get_matches(self):
        for root in self.__roots:
            while root.has_partial_matches():
                yield root.consume_first_partial_match()

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




