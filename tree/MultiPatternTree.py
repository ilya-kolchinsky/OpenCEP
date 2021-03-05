from typing import Dict

from base.Pattern import Pattern
from plan.TreePlan import TreePlan
from tree.PatternMatchStorage import TreeStorageParameters
from tree.Tree import Tree
from tree.nodes.NegationNode import NegationNode


class MultiPatternTree:
    """
    Represents a multi-pattern evaluation tree.
    """

    def __init__(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                 storage_params: TreeStorageParameters):
        self.__pattern_to_output_node_dict = {}
        self.__output_nodes = self.__construct_multi_pattern_tree(pattern_to_tree_plan_map, storage_params)
        self.__id_to_pattern_map = {pattern.id: pattern for pattern in pattern_to_tree_plan_map.keys()}

    def __construct_multi_pattern_tree(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                                       storage_params: TreeStorageParameters):
        """
        Constructing a multi-pattern evaluation tree according to the approach.
        """
        return self.__construct_tree_unified_tree_plan(pattern_to_tree_plan_map, storage_params)

    def __construct_unified_trees(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                                  storage_params: TreeStorageParameters):
        """
        Creates unified treePlan corresponding to the specified tree plans.
        """
        i = 1  # pattern IDs starts from 1
        trees = []
        plan_nodes_to_nodes_map = {}
        for pattern, plan in pattern_to_tree_plan_map.items():
            # passing another parameter (dict )  to the tree constructor
            # means we need to merge trees with the given dict and not just creating a tree
            trees.append(Tree(plan, pattern, storage_params, i, plan_nodes_to_nodes_map))
            i += 1
        return trees

    def __construct_tree_unified_tree_plan(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                                           storage_params: TreeStorageParameters):
        """
        This method gets patterns, builds a single-pattern tree to each one of them,
        and merges equivalent leaves from different trees.
        We are not sharing leaves from the same tree.
        We are assuming that each pattern appears only once in patterns (which is a legitimate assumption).
        """
        trees = self.__construct_unified_trees(pattern_to_tree_plan_map, storage_params)
        self.__output_nodes = []
        # a map between a leaf and the number of equal leaves that were
        # shared to this leaf in the current iteration
        for i, tree in enumerate(trees):
            curr_root = tree.get_root()
            pattern_id = i + 1
            self.__pattern_to_output_node_dict[pattern_id] = curr_root
            self.__output_nodes.append(curr_root)
        return self.__output_nodes

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
