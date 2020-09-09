"""
This file contains the implementations of algorithms constructing a generic (bushy) tree-based evaluation mechanism.
"""
from typing import List

from evaluation.EvaluationMechanismBuilder import EvaluationMechanismBuilder
from tree.PatternMatchStorage import TreeStorageParameters
from tree.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism
from base.Pattern import Pattern
from misc.Utils import get_all_disjoint_sets
from misc.Statistics import calculate_bushy_tree_cost_function, MissingStatisticsException
from misc.StatisticsTypes import StatisticsTypes
from plan.LeftDeepTreeBuilders import GreedyLeftDeepTreeBuilder
from itertools import combinations


class BushyTreeBuilder(EvaluationMechanismBuilder):
    """
    An abstract class for left-deep tree builders.
    """
    def build_single_pattern_eval_mechanism(self, pattern: Pattern, storage_params: TreeStorageParameters):
        if pattern.statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            (selectivityMatrix, arrivalRates) = pattern.statistics
        else:
            raise MissingStatisticsException()
        tree_structure = self._find_tree(selectivityMatrix, arrivalRates, pattern.window.total_seconds())
        return TreeBasedEvaluationMechanism(pattern, tree_structure, storage_params)

    def build_multi_pattern_eval_mechanism(self, patterns: List[Pattern], storage_params: TreeStorageParameters):
        raise Exception("Unsupported")

    @staticmethod
    def _find_tree(selectivity_matrix: List[List[float]], arrival_rates: List[int], window: int):
        raise NotImplementedError()


class DynamicProgrammingBushyTreeBuilder(BushyTreeBuilder):
    """
    Creates a left-deep tree using a dynamic programming algorithm.
    """
    @staticmethod
    def _find_tree(selectivity_matrix: List[List[float]], arrival_rates: List[int], window: int):
        args_num = len(selectivity_matrix)
        if args_num == 1:
            return [0]

        items = frozenset(range(args_num))
        # Save subsets' optimal topologies, the cost and the left to add items.
        sub_trees = {frozenset({i}): (i,
                                      calculate_bushy_tree_cost_function(i, selectivity_matrix, arrival_rates, window),
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
                new_tree_ = (tree1_, tree2_)
                new_cost_ = calculate_bushy_tree_cost_function(new_tree_, selectivity_matrix, arrival_rates, window)
                new_left_ = items.difference({subset})
                sub_trees[subset] = new_tree_, new_cost_, new_left_
                # find the best topology based on previous topologies for smaller subsets.
                for set1, set2 in disjoint_sets_iter:
                    tree1, _, _ = sub_trees[set1]
                    tree2, _, _ = sub_trees[set2]
                    new_tree = (tree1, tree2)
                    new_cost = calculate_bushy_tree_cost_function(new_tree, selectivity_matrix, arrival_rates, window)
                    _, cost, left = sub_trees[subset]
                    # if new subset's topology is better, then update to it.
                    if new_cost < cost:
                        sub_trees[subset] = new_tree, new_cost, left
        return sub_trees[items][0]  # return the best topology (index 0 at tuple) for items - the set of all arguments.


class ZStreamTreeBuilder(BushyTreeBuilder):
    """
    Creates a left-deep tree using ZStream algorithm.
    """
    def _find_tree(self, selectivity_matrix: List[List[float]], arrival_rates: List[int], window: int):
        order = self._get_initial_order(selectivity_matrix, arrival_rates)
        return self.__find_tree_for_order(order, selectivity_matrix, arrival_rates, window)

    @staticmethod
    def _get_initial_order(selectivity_matrix: List[List[float]], arrival_rates: List[int]):
        return list(range(len(selectivity_matrix)))

    @staticmethod
    def __find_tree_for_order(order: List[int], selectivity_matrix: List[List[float]],
                              arrival_rates: List[int], window: int):
        args_num = len(order)
        items = tuple(order)
        suborders = {
            (i,): (i, calculate_bushy_tree_cost_function(i, selectivity_matrix, arrival_rates, window))
            for i in items
        }

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
                tree = (tree1_, tree2_)
                cost = calculate_bushy_tree_cost_function(tree, selectivity_matrix, arrival_rates, window)
                suborders[suborder] = tree, cost
                # iterate over splits of suborder
                for k in range(2, i):
                    # find the optimal topology of this split, according to optimal topologies of subsplits.
                    order1, order2 = suborder[:k], suborder[k:]
                    tree1, _ = suborders[order1]
                    tree2, _ = suborders[order2]
                    _, prev_cost = suborders[suborder]
                    new_tree = (tree1, tree2)
                    new_cost = calculate_bushy_tree_cost_function(new_tree, selectivity_matrix, arrival_rates, window)
                    if new_cost < prev_cost:
                        suborders[suborder] = new_tree, new_cost
        return suborders[items][0]  # return the topology (index 0 at tuple) of the entire order, indexed to 'items'.


class ZStreamOrdTreeBuilder(ZStreamTreeBuilder):
    """
    Creates a left-deep tree using ZStream algorithm with the leaf order obtained using an order-based greedy algorithm.
    """
    @staticmethod
    def _get_initial_order(selectivity_matrix: List[List[float]], arrival_rates: List[int]):
        return GreedyLeftDeepTreeBuilder.calculate_greedy_order(selectivity_matrix, arrival_rates)
