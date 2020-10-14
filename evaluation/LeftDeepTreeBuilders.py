"""
This file contains the implementations of algorithms constructing a left-deep tree-based evaluation mechanism.
"""
from enum import Enum
import random
from typing import List
from evaluation.IterativeImprovement import IterativeImprovementType, IterativeImprovementAlgorithmBuilder
from evaluation.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism
from evaluation.EvaluationMechanismBuilder import EvaluationMechanismBuilder
from base.Pattern import Pattern
from misc.Statistics import calculate_left_deep_tree_cost_function, MissingStatisticsException
from statisticsCollector.StatisticsTypes import StatisticsTypes
from misc.Utils import get_order_by_occurrences
from copy import deepcopy
from statisticsCollector.Stat import Stat
from statisticsCollector.StatisticsCollector import StatisticsCollector


class LeftDeepTreeBuilder(EvaluationMechanismBuilder):
    """
    An abstract class for left-deep tree builders.
    """
    def build_single_pattern_eval_mechanism(self, pattern: Pattern, stat: Stat = None,
                                            statistics_collector: StatisticsCollector = None):
        order = self._create_evaluation_order(pattern, stat)
        tree_structure = self.__build_tree_from_order(order)
        return TreeBasedEvaluationMechanism(pattern, tree_structure, statistics_collector), tree_structure

    def build_multi_pattern_eval_mechanism(self, patterns: List[Pattern]):
        raise Exception("Unsupported")

    @staticmethod
    def __build_tree_from_order(order: List[int]):
        """
        Builds a left-deep tree structure from a given order.
        """
        ret = order[0]
        for i in range(1, len(order)):
            ret = (ret, order[i])
        return ret

    def _create_evaluation_order(self, pattern: Pattern, stat: Stat):
        """
        To be implemented by subclasses.
        """
        raise NotImplementedError()


class TrivialLeftDeepTreeBuilder(LeftDeepTreeBuilder):
    """
    Creates a left-deep tree following the pattern-specified order.
    """
    def _create_evaluation_order(self, pattern: Pattern, stat: Stat):
        args_num = len(pattern.structure.args)
        return list(range(args_num))


class AscendingFrequencyTreeBuilder(LeftDeepTreeBuilder):
    """
    Creates a left-deep tree following the order of ascending arrival rates of the event types.
    """
    def _create_evaluation_order(self, pattern: Pattern, stat: Stat):
        statistics_type = pattern.statistics_type if stat is None else stat.statistics_type
        if statistics_type == StatisticsTypes.FREQUENCY_DICT:
            frequency_dict = pattern.statistics
            order = get_order_by_occurrences(pattern.structure.args, frequency_dict)
        elif statistics_type == StatisticsTypes.ARRIVAL_RATES:
            arrival_rates = pattern.statistics if stat is None else stat.arrival_rates
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
    def _create_evaluation_order(self, pattern: Pattern, stat: Stat):
        statistics_type = pattern.statistics_type if stat is None else stat.statistics_type
        if statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            if stat is None:
                (selectivityMatrix, arrivalRates) = pattern.statistics
            else:
                selectivityMatrix = stat.selectivity_matrix
                arrivalRates = stat.arrival_rates

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
            min_change_factor = selectivity_matrix[to_add][to_add] * arrival_rates[to_add]
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


class AdaptiveGreedyLeftDeepTreeBuilder(LeftDeepTreeBuilder):
    """
    Creates a left-deep tree using a greedy strategy that selects at each step the event type that minimizes the cost
    function.
    """
    def __init__(self, optimizer):
        super().__init__()
        self.optimizer = optimizer

    def _create_evaluation_order(self, pattern: Pattern, stat: Stat):
        if stat.statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            selectivityMatrix = stat.selectivity_matrix
            arrivalRates = stat.arrival_rates
        else:
            raise MissingStatisticsException()
        return self.calculate_greedy_order(selectivityMatrix, arrivalRates)

    def calculate_greedy_order(self, selectivity_matrix: List[List[float]], arrival_rates: List[int]):
        """
        At any step we will only consider the intermediate partial matches size,
        even without considering the sliding window, because the result is independent of it.
        For each unselected item, we will calculate the speculated
        effect to the partial matches, and choose the one with minimal increase.
        We don't even need to calculate the cost function.
        """
        invariants = []
        size = len(selectivity_matrix)
        if size == 1:
            return [0]

        new_order = []
        left_to_add = set(range(len(selectivity_matrix)))
        while len(left_to_add) > 0:
            # create first nominee to add.
            to_add = to_add_start = left_to_add.pop()
            min_change_factor = selectivity_matrix[to_add][to_add] * arrival_rates[to_add]
            for j in new_order:
                min_change_factor *= selectivity_matrix[to_add][j]
            smallest_difference = min_change_factor  # The initial value. Higher than the values that need to be checked later

            if len(left_to_add) != 0:
                minimal_invariant_in_iteration = (to_add, list(left_to_add)[0], deepcopy(new_order))
                # find minimum change factor and its according index.
                for i in left_to_add:
                    change_factor = selectivity_matrix[i][i] * arrival_rates[i]
                    for j in new_order:
                        change_factor *= selectivity_matrix[i][j]
                    if change_factor < min_change_factor:
                        smallest_difference = min_change_factor - change_factor
                        minimal_invariant_in_iteration = (i, to_add, deepcopy(new_order))
                        min_change_factor = change_factor
                        to_add = i
                    else:
                        if change_factor - min_change_factor < smallest_difference:
                            minimal_invariant_in_iteration = (to_add, i, deepcopy(new_order))
                            smallest_difference = change_factor - min_change_factor
            else:  # left to add is empty
                minimal_invariant_in_iteration = (to_add, None, None)
            invariants.append(minimal_invariant_in_iteration)
            new_order.append(to_add)

            # if it wasn't the first nominee, then we need to fix the starting speculation we did.
            if to_add != to_add_start:
                left_to_add.remove(to_add)
                left_to_add.add(to_add_start)

        self.optimizer.reoptimizing_decision.set_new_invariants(invariants)
        return new_order


class IterativeImprovementInitType(Enum):
    RANDOM = 0
    GREEDY = 1


class IterativeImprovementLeftDeepTreeBuilder(LeftDeepTreeBuilder):
    """
    Creates a left-deep tree using the iterative improvement procedure.
    """
    def __init__(self, step_limit: int,
                 ii_type: IterativeImprovementType = IterativeImprovementType.SWAP_BASED,
                 init_type: IterativeImprovementInitType = IterativeImprovementInitType.RANDOM):
        self.__iterative_improvement = IterativeImprovementAlgorithmBuilder.create_ii_algorithm(ii_type)
        self.__initType = init_type
        self.__step_limit = step_limit

    def _create_evaluation_order(self, pattern: Pattern, stat: Stat):
        statistics_type = pattern.statistics_type if stat is None else stat.statistics_type
        if statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            if stat is None:
                (selectivityMatrix, arrivalRates) = pattern.statistics
            else:
                selectivityMatrix = stat.selectivity_matrix
                arrivalRates = stat.arrival_rates
        else:
            raise MissingStatisticsException()
        order = None
        if self.__initType == IterativeImprovementInitType.RANDOM:
            order = self.__get_random_order(len(arrivalRates))
        elif self.__initType == IterativeImprovementInitType.GREEDY:
            order = GreedyLeftDeepTreeBuilder.calculate_greedy_order(selectivityMatrix, arrivalRates)
        return self.__iterative_improvement.execute(self.__step_limit, order, selectivityMatrix, arrivalRates,
                                                    pattern.window.total_seconds())

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
    def _create_evaluation_order(self, pattern: Pattern, stat: Stat):
        statistics_type = pattern.statistics_type if stat is None else stat.statistics_type
        if statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            if stat is None:
                (selectivityMatrix, arrivalRates) = pattern.statistics
            else:
                selectivityMatrix = stat.selectivity_matrix
                arrivalRates = stat.arrival_rates
        else:
            raise MissingStatisticsException()
        return DynamicProgrammingLeftDeepTreeBuilder.find_order(selectivityMatrix, arrivalRates,
                                                                pattern.window.total_seconds())

    @staticmethod
    def find_order(selectivity_matrix: List[List[float]], arrival_rates: List[int], window: int):
        args_num = len(selectivity_matrix)
        if args_num == 1:  # boring extreme case
            return [0]

        items = frozenset(range(args_num))
        # Save subsets' optimal orders, the cost and the left to add items.
        sub_orders = {frozenset({i}): ([i],
                                       calculate_left_deep_tree_cost_function([i], selectivity_matrix, arrival_rates,
                                                                              window),
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
                    new_cost = calculate_left_deep_tree_cost_function(order, selectivity_matrix, arrival_rates, window)
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
