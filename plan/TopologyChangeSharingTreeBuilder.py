"""
This file contains the implementations of algorithms constructing a left-deep tree-based evaluation mechanism.
"""
import math
from typing import List, Dict

from nltk import unique_list

from base.Pattern import Pattern
from misc.DefaultConfig import DEFAULT_TREE_COST_MODEL
from misc.Utils import product
from plan.SubTreeSharingTreeBuilder import SubTreeSharingTreeBuilder
from plan.TreeCostModels import TreeCostModels
from plan.TreePlan import TreePlan
from plan.TreePlanBuilderOrders import TreePlanBuilderOrder
from plan.UnifiedTreeBuilder import UnifiedTreeBuilder


class TopologyChangeSharingTreeBuilder(UnifiedTreeBuilder):
    """
    A class for deep tree builders.
    """

    def __init__(self, cost_model_type: TreeCostModels = DEFAULT_TREE_COST_MODEL,
                 tree_plan_order_approach: TreePlanBuilderOrder = TreePlanBuilderOrder.LEFT_TREE):
        super(UnifiedTreeBuilder, self).__init__(cost_model_type)
        self.tree_plan_build_approach = tree_plan_order_approach
        self.trees_number_nodes_shared = 0

    def _union_tree_plans(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan):
        if isinstance(pattern_to_tree_plan_map, TreePlan) or len(pattern_to_tree_plan_map) <= 1:
            return pattern_to_tree_plan_map

        pattern_to_tree_plan_map_copy = pattern_to_tree_plan_map.copy()
        return self.__construct_subtrees_change_topology_tree_plan(pattern_to_tree_plan_map_copy)

    def build_ordered_tree_plans(self, patterns: List[Pattern]):
        """
        Creates a tree-based evaluation plan for the given pattern.
        """
        orders = self.find_matches_orders(patterns)
        trees = {pattern: TreePlan(self._order_to_tree_topology(orders[i], pattern)) for i, pattern in
                 enumerate(patterns)}
        return trees

    def __construct_subtrees_change_topology_tree_plan(self,
                                                       pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan):
        """
        This method gets patterns, builds a single-pattern tree to each one of them,
        and merges equivalent subtrees from different trees.
        We are sharing the maximal subtree that exists in the tree.
        Additional: we change tree plan topology trying to get maximum union
        We are assuming that each pattern appears only once in patterns (which is a legitimate assumption).
        """
        if len(pattern_to_tree_plan_map) <= 1:
            return pattern_to_tree_plan_map
        patterns = list(pattern_to_tree_plan_map.keys())
        pattern1, pattern2 = patterns[0], patterns[1]
        unified_tree_map, max_intersection = self._two_patterns_max_merge(pattern1, pattern2)

        self.trees_number_nodes_shared = max_intersection
        best_orders = self.find_matches_orders(list(pattern_to_tree_plan_map.keys()))
        if len(pattern_to_tree_plan_map) <= 2:
            return unified_tree_map

        for i, pattern in list(enumerate(patterns))[2:]:
            current_unified_tree_map, max_intersection = self._append_pattern_to_multi_tree(i, unified_tree_map,
                                                                                            best_orders[i],
                                                                                            pattern_to_tree_plan_map)
            unified_tree_map[pattern] = current_unified_tree_map[pattern]
            self.trees_number_nodes_shared += max_intersection
        return unified_tree_map

    def _two_patterns_max_merge(self, first_pattern: Pattern, second_pattern: Pattern):
        """
        This method gets two patterns, and tree to each one of them,
        and merges equivalent subtrees from different trees. then we try changing topology and merge again
        We are sharing the maximal subtree that exists in the tree.
        """
        builders = UnifiedTreeBuilder.create_ordered_tree_builders()
        tree_plan_build_approaches = builders.keys()
        first_order, second_order = self.find_matches_orders([first_pattern, second_pattern])

        union_builder = SubTreeSharingTreeBuilder()
        max_intersection = - math.inf

        best_unified = None
        for first_approach, second_approach in product(tree_plan_build_approaches, tree_plan_build_approaches):
            first_builder = builders.get(first_approach)
            second_builder = builders.get(second_approach)
            first_tree = TreePlan(first_builder._order_to_tree_topology(first_order, first_pattern))
            second_tree = TreePlan(second_builder._order_to_tree_topology(second_order, second_pattern))
            first_tree_size = first_builder._sub_tree_size(first_tree.root)
            tree2_size = second_builder._sub_tree_size(second_tree.root)
            pattern_to_tree_plan_map = {first_pattern: first_tree, second_pattern: second_tree}
            unified = union_builder._union_tree_plans(pattern_to_tree_plan_map)
            trees_number_nodes_shared = union_builder.trees_number_nodes_shared
            if trees_number_nodes_shared > max_intersection:
                max_intersection = trees_number_nodes_shared
                best_unified = unified
            if max_intersection >= min(first_tree_size, tree2_size):
                # we got the max intersection that could be
                break

        return best_unified, max_intersection

    def _append_pattern_to_multi_tree(self, pattern_idx, unified_pattern_to_tree_plan_map, best_order: List[int],
                                      pattern_to_tree_plan_map):
        """
        This method gets two pattern_to_tree_plan_map, and pattern,
        and merges equivalent subtrees from different pattern tree plan to the const unified multi tree of the other patterns.
        then we try changing topology and merge again
        We are sharing the maximal subtree that exists in the tree.
        """
        builders = UnifiedTreeBuilder.create_ordered_tree_builders()
        tree_plan_build_approaches = builders.keys()

        patterns = list(pattern_to_tree_plan_map.keys())
        current_pattern = patterns[pattern_idx]

        best_unified_tree_map, max_intersection = None, -math.inf
        for pattern in unified_pattern_to_tree_plan_map:
            unified_tree_map, cur_intersection = self._two_patterns_max_merge(pattern, current_pattern)
            if cur_intersection >= max_intersection:
                max_intersection = cur_intersection
                best_unified_tree_map = unified_tree_map.copy()

        for pattern in unified_pattern_to_tree_plan_map:
            for approach in tree_plan_build_approaches:
                builder = builders.get(approach)
                tree = TreePlan(builder._order_to_tree_topology(best_order, current_pattern))
                tree_size = builder._sub_tree_size(tree.root)

                pattern_to_tree_plan_map[current_pattern] = tree
                unified_tree_map, cur_intersection = self._two_patterns_max_merge(pattern, current_pattern)

                if cur_intersection > max_intersection:
                    max_intersection = cur_intersection
                    best_unified_tree_map = unified_tree_map.copy()

                if max_intersection >= tree_size:
                    # we got the max intersection that could be
                    break

        return best_unified_tree_map, max_intersection

    @staticmethod
    def find_matches_orders(patterns: List[Pattern]):
        """
        """
        if len(patterns) <= 1:
            return UnifiedTreeBuilder._create_evaluation_order(patterns[0])

        first_two_orders = TopologyChangeSharingTreeBuilder.find_orders_for_two_patterns(patterns[0], patterns[1])
        orders = first_two_orders
        for i, pattern in list(enumerate(patterns))[2:]:
            orders += [TopologyChangeSharingTreeBuilder.find_order_for_new_pattern(patterns[0:i], pattern)]
        return orders

    @staticmethod
    def find_orders_for_two_patterns(first_pattern: Pattern, second_pattern: Pattern):
        """
        """

        first_pattern_events = first_pattern.positive_structure.get_args()
        second_pattern_events = second_pattern.positive_structure.get_args()
        shared = []

        for event1 in first_pattern_events:
            for event2 in second_pattern_events:
                if event1.type == event2.type and TopologyChangeSharingTreeBuilder.are_conditions_equal(first_pattern, event1.name,
                                                                                          second_pattern, event2.name):
                    shared += [(event1.name, event2.name, first_pattern_events.index(event1), second_pattern_events.index(event2))]
                    break

        if len(shared) == 0:
            first_order = list(range(len(first_pattern.positive_structure.args)))
            second_order = list(range(len(second_pattern.positive_structure.args)))
            return [first_order, second_order]

        _, _, first_order, second_order = list(zip(*shared))
        first_order = unique_list(first_order) + list(filter(lambda x: x not in first_order, range(len(first_pattern_events))))
        second_order = unique_list(second_order) + list(filter(lambda x: x not in second_order, range(len(second_pattern_events))))
        return [first_order, second_order]

    @staticmethod
    def are_conditions_equal(first_pattern, first_event_name, second_pattern, second_event_name):
        first_condition = first_pattern.condition.get_condition_of(first_event_name, get_kleene_closure_conditions=False,
                                                                   consume_returned_conditions=False)
        second_condition = second_pattern.condition.get_condition_of(second_event_name, get_kleene_closure_conditions=False,
                                                                     consume_returned_conditions=False)
        return first_condition == second_condition

    @staticmethod
    def find_order_for_new_pattern(patterns: List[Pattern], new_pattern: Pattern):
        """
        """

        max_order_len = - math.inf
        best_order = []
        for i, pattern in enumerate(patterns):
            _, order = TopologyChangeSharingTreeBuilder.find_orders_for_two_patterns(pattern, new_pattern)
            if len(order) >= max_order_len:
                max_order_len = len(order)
                best_order = order

        return best_order
