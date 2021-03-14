"""
This file contains the implementations of algorithms constructing an invariant aware (bushy) tree-based evaluation mechanism.
"""
from typing import List, Dict

from adaptive.statistics.StatisticsTypes import StatisticsTypes
from plan.invariant.InvariantTreePlanBuilder import InvariantTreePlanBuilder
from plan.invariant.Invariants import Invariant, ZStreamTreeInvariants
from plan.TreePlan import TreePlanLeafNode, TreePlanNode
from plan.TreePlanBuilder import TreePlanBuilder
from base.Pattern import Pattern
from misc.LegacyStatistics import MissingStatisticsException


class InvariantAwareZStreamTreeBuilder(InvariantTreePlanBuilder):
    """
    Creates an invariant aware bushy tree using ZStream algorithm.
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
        suborders = {
            (i,): (TreePlanLeafNode(i), self._get_plan_cost(pattern, TreePlanLeafNode(i), statistics))
            for i in items
        }

        tree_to_second_min_tree_map = {}
        invariants = ZStreamTreeInvariants(self._get_plan_cost)
        all_sub_trees = []

        # iterate over suborders sizes
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
                    new_cost = self._get_plan_cost(pattern, new_tree, statistics)
                    if new_cost < prev_cost:
                        second_prev_cost = prev_cost
                        second_min_tree = suborders[suborder][0]

                        suborders[suborder] = new_tree, new_cost

                    elif new_cost < second_prev_cost or second_min_tree == tree:
                        second_prev_cost = new_cost
                        second_min_tree = new_tree

                if i != 2:
                    tree_to_second_min_tree_map[suborders[suborder][0]] = second_min_tree

        # Eliminates all trees that are not in best tree from map_tree_to_second_min_tree
        InvariantAwareZStreamTreeBuilder.__get_relevant_sub_trees(suborders[items][0], all_sub_trees)

        for tree in all_sub_trees:
            invariants.add(Invariant(tree, tree_to_second_min_tree_map[tree]))

        # return the topology (index 0 at tuple) of the entire order, indexed to 'items'
        return suborders[items][0], invariants

    @staticmethod
    def _get_initial_order(selectivity_matrix: List[List[float]], arrival_rates: List[int]):
        return list(range(len(selectivity_matrix)))

    @staticmethod
    def __get_relevant_sub_trees(tree, all_sub_trees):
        """
        Unlike the invariant aware greedy tree builder,
        here we want to eliminate all trees that are not in the best tree.
        We only care about trees that have at least 3 leaf nodes.
        """
        if isinstance(tree, TreePlanLeafNode) or \
                (isinstance(tree.left_child, TreePlanLeafNode) and isinstance(tree.right_child, TreePlanLeafNode)):
            return

        all_sub_trees.append(tree)

        InvariantAwareZStreamTreeBuilder.__get_relevant_sub_trees(tree.left_child, all_sub_trees)
        InvariantAwareZStreamTreeBuilder.__get_relevant_sub_trees(tree.right_child, all_sub_trees)
