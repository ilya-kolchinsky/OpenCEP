"""
This file contains the implementations of algorithms constructing a generic (bushy) tree-based evaluation mechanism.
"""
from typing import List

from plan.Invariants import Invariant, ZStreamTreeInvariants
from plan.TreePlan import TreePlanLeafNode, TreePlan
from plan.TreePlanBuilder import TreePlanBuilder
from base.Pattern import Pattern
from misc.Utils import get_all_disjoint_sets
from misc.Statistics import MissingStatisticsException
from misc.StatisticsTypes import StatisticsTypes
from plan.LeftDeepTreeBuilders import GreedyLeftDeepTreeBuilder
from itertools import combinations

from statistics_collector.NewStatistics import Statistics, SelectivityAndArrivalRatesStatistics
from statistics_collector.StatisticsObjects import StatisticsObject, SelectivityMatrixAndArrivalRates


class InvariantAwareZStreamTreeBuilder(TreePlanBuilder):
    """
    Creates a bushy tree using ZStream algorithm.
    """
    def build_tree_plan(self, statistics: StatisticsObject, pattern: Pattern):
        """
        Creates a tree-based evaluation plan for the given pattern.
        """
        tree_topology, invariants = self._create_tree_topology(statistics, pattern)
        return TreePlan(tree_topology), invariants

    def _create_tree_topology(self, statistics: StatisticsObject, pattern: Pattern):
        if isinstance(statistics, SelectivityMatrixAndArrivalRates):
            (selectivity_matrix, arrival_rates) = statistics.statistics
        else:
            raise MissingStatisticsException()

        order = self._get_initial_order(selectivity_matrix, arrival_rates)
        args_num = len(order)
        items = tuple(order)
        suborders = {
            (i,): (TreePlanLeafNode(i), self._get_plan_cost(statistics, pattern, TreePlanLeafNode(i)))
            for i in items
        }

        tree_to_second_min_tree_map = {}
        cost_model = self.get_cost_model()
        invariants = ZStreamTreeInvariants(cost_model)
        all_sub_trees = []

        # iterate over suborders' sizes
        for i in range(2, args_num + 1):
            # iterate over suborders of size i
            for j in range(args_num - i + 1):
                # create the suborder (slice) to find its optimum.
                suborder = tuple(order[t] for t in range(j, j + i))
                # use first split of suborder as speculative best.
                order1_, order2_ = suborder[:1], suborder[1:]
                tree1_, _ = suborders[order1_]
                tree2_, _ = suborders[order2_]
                tree = TreePlanBuilder._instantiate_binary_node(pattern, tree1_, tree2_)
                cost = self._get_plan_cost(statistics, pattern, tree)
                suborders[suborder] = tree, cost

                second_prev_cost = cost
                second_min_tree = tree

                # iterate over splits of suborder
                for k in range(2, i):

                    # find the optimal topology of this split, according to optimal topologies of subsplits.
                    order1, order2 = suborder[:k], suborder[k:]
                    tree1, _ = suborders[order1]
                    tree2, _ = suborders[order2]
                    _, prev_cost = suborders[suborder]
                    new_tree = TreePlanBuilder._instantiate_binary_node(pattern, tree1, tree2)
                    new_cost = self._get_plan_cost(statistics, pattern, new_tree)
                    if new_cost < prev_cost:
                        second_prev_cost = prev_cost
                        second_min_tree = suborders[suborder][0]

                        suborders[suborder] = new_tree, new_cost

                    elif new_cost < second_prev_cost or second_min_tree == tree:
                        second_prev_cost = new_cost
                        second_min_tree = new_tree

                if i != 2:
                    tree_to_second_min_tree_map[suborders[suborder][0]] = second_min_tree

        # Eliminate from trees in map_tree_to_second_min_tree that are not exist in the best tree
        InvariantAwareZStreamTreeBuilder.get_all_sub_trees(suborders[items][0],
                                                           tree_to_second_min_tree_map, all_sub_trees)

        for tree in all_sub_trees:
            invariants.add(Invariant(tree, tree_to_second_min_tree_map[tree]))

        # return the topology (index 0 at tuple) of the entire order, indexed to 'items'
        return suborders[items][0], invariants

    @staticmethod
    def _get_initial_order(selectivity_matrix: List[List[float]], arrival_rates: List[int]):
        return list(range(len(selectivity_matrix)))

    @staticmethod
    def get_all_sub_trees(tree, tree_to_second_min_tree_map, all_sub_trees):
        """
        We care about trees with at least 3 leaf node
        """
        if isinstance(tree, TreePlanLeafNode) or \
                (isinstance(tree.left_child, TreePlanLeafNode) and isinstance(tree.right_child, TreePlanLeafNode)):
            return

        all_sub_trees.append(tree)

        InvariantAwareZStreamTreeBuilder.get_all_sub_trees(tree.left_child, tree_to_second_min_tree_map, all_sub_trees)

        InvariantAwareZStreamTreeBuilder.get_all_sub_trees(tree.right_child, tree_to_second_min_tree_map, all_sub_trees)
