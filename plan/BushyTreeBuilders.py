"""
This file contains the implementations of algorithms constructing a generic (bushy) tree-based evaluation mechanism.
"""
from typing import List, Dict

from plan.TreePlan import TreePlanNode
from plan.TreePlanBuilder import TreePlanBuilder
from base.Pattern import Pattern
from misc.Utils import get_all_disjoint_sets
from misc.LegacyStatistics import MissingStatisticsException
from adaptive.statistics.StatisticsTypes import StatisticsTypes
from plan.LeftDeepTreeBuilders import GreedyLeftDeepTreeBuilder
from itertools import combinations


class DynamicProgrammingBushyTreeBuilder(TreePlanBuilder):
    """
    Creates a bushy tree using a dynamic programming algorithm.
    """
    def _create_tree_topology(self, pattern: Pattern, statistics: Dict, leaves: List[TreePlanNode]):
        if StatisticsTypes.ARRIVAL_RATES in statistics and \
                StatisticsTypes.SELECTIVITY_MATRIX in statistics and \
                len(statistics) == 2:
            selectivity_matrix = statistics[StatisticsTypes.SELECTIVITY_MATRIX]
            arrival_rates = statistics[StatisticsTypes.ARRIVAL_RATES]
        else:
            raise MissingStatisticsException()

        args_num = len(selectivity_matrix)
        if args_num == 1:
            return leaves[0]

        items = frozenset(range(args_num))
        # Save subsets' optimal topologies, the cost and the left to add items.
        # In case of a nested node and not a real Primitive, creates nested node instead
        sub_trees = {frozenset({i}): (leaves[i],
                                      self._get_plan_cost(pattern, leaves[i], statistics),
                                      items.difference({i}))
                     for i in items}

        # for each subset of size i, find optimal topology for these subsets according to size (i-1) subsets.
        for i in range(2, args_num + 1):
            for tSubset in combinations(items, i):
                subset = frozenset(tSubset)
                disjoint_sets_iter = get_all_disjoint_sets(subset)  # iterator for all disjoint splits of a set.
                # use first option as speculative best.
                set1_, set2_ = next(disjoint_sets_iter)
                tree1_, _, _ = sub_trees[set1_]
                tree2_, _, _ = sub_trees[set2_]
                new_tree_ = TreePlanBuilder._instantiate_binary_node(pattern, tree1_, tree2_)
                new_cost_ = self._get_plan_cost(pattern, new_tree_, statistics)
                new_left_ = items.difference({subset})
                sub_trees[subset] = new_tree_, new_cost_, new_left_
                # find the best topology based on previous topologies for smaller subsets.
                for set1, set2 in disjoint_sets_iter:
                    tree1, _, _ = sub_trees[set1]
                    tree2, _, _ = sub_trees[set2]
                    new_tree = TreePlanBuilder._instantiate_binary_node(pattern, tree1, tree2)
                    new_cost = self._get_plan_cost(pattern, new_tree, statistics)
                    _, cost, left = sub_trees[subset]
                    # if new subset's topology is better, then update to it.
                    if new_cost < cost:
                        sub_trees[subset] = new_tree, new_cost, left
        # the best topology (index 0 at tuple) for items - the set of all arguments
        return sub_trees[items][0]


class ZStreamTreeBuilder(TreePlanBuilder):
    """
    Creates a bushy tree using ZStream algorithm.
    """
    def _create_tree_topology(self, pattern: Pattern, statistics: Dict, leaves: List[TreePlanNode]):
        if StatisticsTypes.ARRIVAL_RATES in statistics and \
                StatisticsTypes.SELECTIVITY_MATRIX in statistics and \
                len(statistics) == 2:
            selectivity_matrix = statistics[StatisticsTypes.SELECTIVITY_MATRIX]
            arrival_rates = statistics[StatisticsTypes.ARRIVAL_RATES]
        else:
            raise MissingStatisticsException()

        order = self._get_initial_order(selectivity_matrix, arrival_rates)
        args_num = len(order)
        items = tuple(order)
        suborders = {(i,): (leaves[i], self._get_plan_cost(pattern, leaves[i], statistics)) for i in items}

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
                cost = self._get_plan_cost(pattern, tree, statistics)
                suborders[suborder] = tree, cost
                # iterate over splits of suborder
                for k in range(2, i):
                    # find the optimal topology of this split, according to optimal topologies of subsplits.
                    order1, order2 = suborder[:k], suborder[k:]
                    tree1, _ = suborders[order1]
                    tree2, _ = suborders[order2]
                    _, prev_cost = suborders[suborder]
                    new_tree = TreePlanBuilder._instantiate_binary_node(pattern, tree1, tree2)
                    new_cost = self._get_plan_cost(pattern, new_tree, statistics)
                    if new_cost < prev_cost:
                        suborders[suborder] = new_tree, new_cost
        return suborders[items][0]  # return the topology (index 0 at tuple) of the entire order, indexed to 'items'.

    @staticmethod
    def _get_initial_order(selectivity_matrix: List[List[float]], arrival_rates: List[int]):
        return list(range(len(selectivity_matrix)))


class ZStreamOrdTreeBuilder(ZStreamTreeBuilder):
    """
    Creates a bushy tree using ZStream algorithm with the leaf order obtained using an order-based greedy algorithm.
    """
    @staticmethod
    def _get_initial_order(selectivity_matrix: List[List[float]], arrival_rates: List[int]):
        return GreedyLeftDeepTreeBuilder.calculate_greedy_order(selectivity_matrix, arrival_rates)
