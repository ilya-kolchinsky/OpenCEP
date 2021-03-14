"""
This file contains the implementations of algorithms constructing invariant-aware left-deep tree-based evaluation mechanism.
"""
from typing import List, Dict

from base.PatternStructure import CompositeStructure
from adaptive.statistics.StatisticsTypes import StatisticsTypes
from plan.invariant.InvariantTreePlanBuilder import InvariantTreePlanBuilder
from plan.invariant.Invariants import Invariant, GreedyTreeInvariants
from plan.TreePlan import TreePlanLeafNode, TreePlanNode
from plan.TreePlanBuilder import TreePlanBuilder
from base.Pattern import Pattern
from misc.LegacyStatistics import MissingStatisticsException


class InvariantLeftDeepTreeBuilder(InvariantTreePlanBuilder):
    """
    An abstract class for left-deep tree builders.
    """
    def _create_tree_topology(self, pattern: Pattern, statistics: Dict, leaves: List[TreePlanNode]):
        order, invariants = self._create_evaluation_order(statistics) if isinstance(pattern.positive_structure,
                                                                                    CompositeStructure) else [[0], None]
        return self._order_to_tree_topology(order, pattern), invariants

    @staticmethod
    def _order_to_tree_topology(order: List[int], pattern: Pattern):
        """
        A helper method for converting a given order to a tree topology.
        """
        tree_topology = TreePlanLeafNode(order[0])
        for i in range(1, len(order)):
            tree_topology = TreePlanBuilder._instantiate_binary_node(pattern, tree_topology, TreePlanLeafNode(order[i]))
        return tree_topology

    def _get_order_cost(self, pattern: Pattern, order: List[int], statistics: Dict):
        """
        Returns the cost of a given order of event processing.
        """
        tree_plan = InvariantLeftDeepTreeBuilder._order_to_tree_topology(order, pattern)
        return self._get_plan_cost(pattern, tree_plan, statistics)

    def _create_evaluation_order(self, statistics: dict):
        """
        Creates an evaluation order to serve as a basis for the invariant aware left-deep tree topology.
        """
        raise NotImplementedError()


class InvariantAwareGreedyTreeBuilder(InvariantLeftDeepTreeBuilder):
    """
    Creates a left-deep tree using a greedy strategy that selects at each step the event type that minimizes the cost
    function.
    """
    def _create_evaluation_order(self, statistics: dict):
        if StatisticsTypes.ARRIVAL_RATES in statistics and \
                StatisticsTypes.SELECTIVITY_MATRIX in statistics and \
                len(statistics) == 2:
            selectivity_matrix = statistics[StatisticsTypes.SELECTIVITY_MATRIX]
            arrival_rates = statistics[StatisticsTypes.ARRIVAL_RATES]
        else:
            raise MissingStatisticsException()
        order, invariants = self.calculate_greedy_order(selectivity_matrix, arrival_rates)

        return order, invariants

    @staticmethod
    def calculate_greedy_order(selectivity_matrix: List[List[float]], arrival_rates: List[int]):
        """
        At any step we only consider the intermediate partial matches size,
        even without considering the sliding window, because the result is independent of it.
        For each unselected item, we calculate the speculated
        effect to the partial matches and choose the one with minimal increase.
        We don't even need to calculate the cost function.
        """
        size = len(selectivity_matrix)
        if size == 1:
            return [0], None

        invariants = GreedyTreeInvariants()

        new_order = []
        left_to_add = set(range(len(selectivity_matrix)))
        while len(left_to_add) > 0:
            # create first nominee to add.
            to_add = to_add_start = left_to_add.pop()
            min_change_factor = selectivity_matrix[to_add][to_add] * arrival_rates[to_add]
            for j in new_order:
                min_change_factor *= selectivity_matrix[to_add][j]

            second_min_change_factor = min_change_factor
            second_min_index = to_add

            # finds the minimum change factor and its corresponding index.
            for i in left_to_add:
                change_factor = selectivity_matrix[i][i] * arrival_rates[i]
                for j in new_order:
                    change_factor *= selectivity_matrix[i][j]
                if change_factor < min_change_factor:

                    second_min_index = to_add
                    second_min_change_factor = min_change_factor

                    min_change_factor = change_factor
                    to_add = i

                # The second condition "second_min_index == to_add"
                # handles the case that in the current while loop,
                # the first index that popped from the "left_to_add" set is the minimum.
                # Hence we need to save the second minimum for the invariants
                # because in this case we never change the second_min_index.
                elif change_factor < second_min_change_factor or second_min_index == to_add:
                    second_min_change_factor = change_factor
                    second_min_index = i

            new_order.append(to_add)
            invariants.add(Invariant(to_add, second_min_index))

            # if it wasn't the first nominee, we need to fix the starting speculation we did.
            if to_add != to_add_start:
                left_to_add.remove(to_add)
                left_to_add.add(to_add_start)

        # Fix the last invariant
        invariants.invariants[-1].right = None

        return new_order, invariants
