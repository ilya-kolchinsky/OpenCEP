from typing import Dict

from base.Pattern import Pattern
from plan.TreePlan import TreePlan
from tree.PatternMatchStorage import TreeStorageParameters
from base.PatternMatch import PatternMatch
from tree.Tree import Tree
from tree.nodes.NegationNode import NegationNode


class MultiPatternTree:
    """
    Represents a multi-pattern evaluation tree.
    """
    def __init__(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                 storage_params: TreeStorageParameters):
        self.__id_to_output_node_map = {}
        self.__id_to_pattern_map = {}
        self.__output_nodes = []
        self.__construct_multi_pattern_tree(pattern_to_tree_plan_map, storage_params)

    def __construct_multi_pattern_tree(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                                       storage_params: TreeStorageParameters):
        """
        Constructs a multi-pattern evaluation tree.
        It is assumed that each pattern appears only once in patterns (which is a legitimate assumption).
        """
        i = 1  # pattern IDs starts from 1
        plan_nodes_to_nodes_map = {}  # a cache for already created subtrees
        for pattern, plan in pattern_to_tree_plan_map.items():
            pattern.id = i
            new_tree_root = Tree(plan, pattern, storage_params, plan_nodes_to_nodes_map).get_root()
            self.__id_to_output_node_map[pattern.id] = new_tree_root
            self.__id_to_pattern_map[pattern.id] = pattern
            self.__output_nodes.append(new_tree_root)
            i += 1

    def get_leaves(self):
        """
        Returns all leaves in this multi-pattern-tree.
        """
        leaves = set()
        for output_node in self.__output_nodes:
            leaves |= set(output_node.get_leaves())
        return leaves

    def __should_attach_match_to_pattern(self, match: PatternMatch, pattern: Pattern):
        """
        Returns True if the given match satisfies the window/confidence constraints of the given pattern
        and False otherwise.
        """
        if match.last_timestamp - match.first_timestamp > pattern.window:
            return False
        return pattern.confidence is None or match.probability is None or match.probability >= pattern.confidence

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
                    if self.__id_to_output_node_map[pattern_id] != output_node:
                        # the current output node is an internal node in pattern #idx, but not it's root.
                        # we don't need to check if there are any matches for this pattern id.
                        continue
                    # check if timestamp is correct for this pattern id.
                    # the pattern indices start from 1.
                    if self.__should_attach_match_to_pattern(match, self.__id_to_pattern_map[pattern_id]):
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
