"""
This file contains the implementations of algorithms constructing a left-deep tree-based evaluation mechanism.
"""
import random
from typing import List
import numpy as np

from base.PatternStructure import PatternStructure, CompositeStructure, PrimitiveEventStructure, \
    SeqOperator, NegationOperator, AndOperator
from misc import DefaultConfig
from plan.IterativeImprovement import IterativeImprovementType, IterativeImprovementInitType, \
    IterativeImprovementAlgorithmBuilder
from plan.TreeCostModels import TreeCostModels
from plan.TreePlan import TreePlanLeafNode, TreePlanNode, TreePlanNestedNode
from plan.TreePlanBuilder import TreePlanBuilder
from base.Pattern import Pattern
from misc.Statistics import MissingStatisticsException
from misc.StatisticsTypes import StatisticsTypes
from misc.Utils import get_order_by_occurrences


class LeftDeepTreeBuilder(TreePlanBuilder):
    """
    An abstract class for left-deep tree builders.
    """

    def _create_tree_topology(self, pattern: Pattern):
        """
        Invokes an algorithm (to be implemented by subclasses) that builds an evaluation order of the operands, and
        converts it into a left-deep tree topology.
        """
        nested_topologies = None
        nested_args = None
        nested_stats = None
        if isinstance(pattern.positive_structure, CompositeStructure):  # TODO: else
            nested_topologies = []
            nested_args = []
            nested_stats = []
            for arg in pattern.positive_structure.get_args():
                if isinstance(arg, CompositeStructure):
                    temp_pattern = Pattern(arg, None, None)
                    if pattern.statistics_type == StatisticsTypes.ARRIVAL_RATES:
                        temp_pattern.set_statistics(StatisticsTypes.ARRIVAL_RATES, pattern.statistics)
                    sub_stats, nested_topology = self._create_tree_topology(temp_pattern)
                    nested_topologies.append(nested_topology)
                    if pattern.statistics_type == StatisticsTypes.ARRIVAL_RATES:
                        nested_stats.append(np.sum(sub_stats))
                    nested_args.append(arg.args)
                else:
                    nested_topologies.append(None)
                    nested_args.append(None)
                    if pattern.statistics_type == StatisticsTypes.ARRIVAL_RATES:
                        nested_stats.append(pattern.statistics[0])
                        pattern.statistics.pop(0)
            if pattern.statistics_type == StatisticsTypes.ARRIVAL_RATES:
                pattern.set_statistics(StatisticsTypes.ARRIVAL_RATES, nested_stats)

        order = self._create_evaluation_order(pattern) if isinstance(pattern.positive_structure,
                                                                     CompositeStructure) else [0]
        return nested_stats, LeftDeepTreeBuilder._order_to_tree_topology(order, pattern, nested_topologies, nested_args)

    @staticmethod
    def _order_to_tree_topology(order: List[int], pattern: Pattern, nested_topologies: List[TreePlanNode] = None, nested_args = None):

        """
        A helper method for converting a given order to a tree topology.
        """
        tree_topology = TreePlanLeafNode(order[0]) if (nested_topologies is None or nested_topologies[order[0]] is None) else TreePlanNestedNode(order[0], nested_topologies[order[0]], nested_args[order[0]])
        for i in range(1, len(order)):
            new_node = TreePlanLeafNode(order[i]) if (nested_topologies is None or nested_topologies[order[i]] is None) else TreePlanNestedNode(order[i], nested_topologies[order[i]],nested_args[order[i]])
            tree_topology = TreePlanBuilder._instantiate_binary_node(pattern, tree_topology, new_node)
        return tree_topology

    def _get_order_cost(self, pattern: Pattern, order: List[int]):
        """
        Returns the cost of a given order of event processing.
        """
        tree_plan = LeftDeepTreeBuilder._order_to_tree_topology(order, pattern)
        return self._get_plan_cost(pattern, tree_plan)

    def _create_evaluation_order(self, pattern: Pattern):
        """
        Creates an evaluation order to serve as a basis for the left-deep tree topology.
        """
        raise NotImplementedError()


class TrivialLeftDeepTreeBuilder(LeftDeepTreeBuilder):
    """
    Creates a left-deep tree following the pattern-specified order.
    """

    def _create_evaluation_order(self, pattern: Pattern):
        args_num = len(pattern.positive_structure.args)
        return list(range(args_num))


class AscendingFrequencyTreeBuilder(LeftDeepTreeBuilder):
    """
    Creates a left-deep tree following the order of ascending arrival rates of the event types.
    """

    def _create_evaluation_order(self, pattern: Pattern):
        if pattern.statistics_type == StatisticsTypes.FREQUENCY_DICT:
            frequency_dict = pattern.statistics
            order = get_order_by_occurrences(pattern.positive_structure.args, frequency_dict)
        elif pattern.statistics_type == StatisticsTypes.ARRIVAL_RATES:
            arrival_rates = pattern.statistics
            # create an index-arrival rate binding and sort according to arrival rate.
            sorted_order = sorted([(i, arrival_rates[i]) for i in range(len(arrival_rates))], key=lambda x: x[1])
            order = [x for x, y in sorted_order]  # create order from sorted binding.
        else:
            raise MissingStatisticsException()
        return order


class GreedyLeftDeepTreeBuilder(LeftDeepTreeBuilder):
    """
    Creates a left-deep tree using a greedy strategy that selects at each step the event type that minimizes the cost
    function.
    """

    def _create_evaluation_order(self, pattern: Pattern):
        if pattern.statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            (selectivityMatrix, arrivalRates) = pattern.statistics
        else:
            raise MissingStatisticsException()
        return self.calculate_greedy_order(selectivityMatrix, arrivalRates)

    @staticmethod
    def calculate_greedy_order(selectivity_matrix: List[List[float]], arrival_rates: List[int]):
        """
        At any step we will only consider the intermediate partial matches size,
        even without considering the sliding window, because the result is independent of it.
        For each unselected item, we will calculate the speculated
        effect to the partial matches, and choose the one with minimal increase.
        We don't even need to calculate the cost function.
        """
        size = len(selectivity_matrix)
        if size == 1:
            return [0]

        new_order = []
        left_to_add = set(range(len(selectivity_matrix)))
        while len(left_to_add) > 0:
            # create first nominee to add.
            to_add = to_add_start = left_to_add.pop()
            min_change_factor = selectivity_matrix[to_add][to_add]
            for j in new_order:
                min_change_factor *= selectivity_matrix[to_add][j]

            # find minimum change factor and its according index.
            for i in left_to_add:
                change_factor = selectivity_matrix[i][i] * arrival_rates[i]
                for j in new_order:
                    change_factor *= selectivity_matrix[i][j]
                if change_factor < min_change_factor:
                    min_change_factor = change_factor
                    to_add = i
            new_order.append(to_add)

            # if it wasn't the first nominee, then we need to fix the starting speculation we did.
            if to_add != to_add_start:
                left_to_add.remove(to_add)
                left_to_add.add(to_add_start)

        return new_order


class IterativeImprovementLeftDeepTreeBuilder(LeftDeepTreeBuilder):
    """
    Creates a left-deep tree using the iterative improvement procedure.
    """

    def __init__(self, cost_model_type: TreeCostModels, step_limit: int,
                 ii_type: IterativeImprovementType = DefaultConfig.ITERATIVE_IMPROVEMENT_TYPE,
                 init_type: IterativeImprovementInitType = DefaultConfig.ITERATIVE_IMPROVEMENT_TYPE):
        super().__init__(cost_model_type)
        self.__iterative_improvement = IterativeImprovementAlgorithmBuilder.create_ii_algorithm(ii_type)
        self.__initType = init_type
        self.__step_limit = step_limit

    def _create_evaluation_order(self, pattern: Pattern):
        if pattern.statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            (selectivityMatrix, arrivalRates) = pattern.statistics
        else:
            raise MissingStatisticsException()
        order = None
        if self.__initType == IterativeImprovementInitType.RANDOM:
            order = self.__get_random_order(len(arrivalRates))
        elif self.__initType == IterativeImprovementInitType.GREEDY:
            order = GreedyLeftDeepTreeBuilder.calculate_greedy_order(selectivityMatrix, arrivalRates)
        get_cost_callback = lambda o: self._get_order_cost(pattern, o)
        return self.__iterative_improvement.execute(self.__step_limit, order, get_cost_callback)

    @staticmethod
    def __get_random_order(n: int):
        """
        Used for creating an initial order in RANDOM mode.
        """
        order = []
        left = list(range(n))
        while len(left) > 0:
            index = random.randint(0, len(left) - 1)
            order.append(left[index])
            del left[index]
        return order


class DynamicProgrammingLeftDeepTreeBuilder(LeftDeepTreeBuilder):
    """
    Creates a left-deep tree using a dynamic programming algorithm.
    """

    def _create_evaluation_order(self, pattern: Pattern):
        if pattern.statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            (selectivity_matrix, arrival_rates) = pattern.statistics
        else:
            raise MissingStatisticsException()
        args_num = len(selectivity_matrix)
        if args_num == 1:  # boring extreme case
            return [0]

        items = frozenset(range(args_num))
        # Save subsets' optimal orders, the cost and the left to add items.
        sub_orders = {frozenset({i}): ([i],
                                       self._get_order_cost(pattern, [i]),
                                       items.difference({i}))
                      for i in items}

        for i in range(2, args_num + 1):
            # for each subset of size i, we will find the best order for each subset
            next_orders = {}
            for subset in sub_orders.keys():
                order, _, left_to_add = sub_orders[subset]
                for item in left_to_add:
                    # calculate for optional order for set of size i
                    new_subset = frozenset(subset.union({item}))
                    new_cost = self._get_order_cost(pattern, order)
                    # check if it is not the first order for that set
                    if new_subset in next_orders.keys():
                        _, t_cost, t_left = next_orders[new_subset]
                        if new_cost < t_cost:  # check if it is the current best order for that set
                            new_order = order + [item]
                            next_orders[new_subset] = new_order, new_cost, t_left
                    else:  # if it is the first order for that set
                        new_order = order + [item]
                        next_orders[new_subset] = new_order, new_cost, left_to_add.difference({item})
            # update subsets for next iteration
            sub_orders = next_orders
        return list(sub_orders.values())[0][
            0]  # return the order (at index 0 in the tuple) of item 0, the only item in subsets of size n.
