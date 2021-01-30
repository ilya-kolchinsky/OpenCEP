from abc import ABC, abstractmethod

from base.Pattern import Pattern
from misc.Statistics import MissingStatisticsException
from plan.TreeCostModel import TreeCostModelFactory, TreeCostModel, IntermediateResultsTreeCostModel
from plan.TreeCostModels import TreeCostModels
from statistics_collector.Statistics import Statistics, SelectivityAndArrivalRatesStatistics
from statistics_collector.StatisticsWrapper import StatisticsWrapper, SelectivityAndArrivalRatesWrapper


class Invariant:
    """
    This class represent single invariant.
    invariant will be defined as an inequality of the following form:
    f1 (stat1) < f2 (stat2)
    Hence, we save the index corresponding to stat1 in the left side of the inequality
    and the index corresponding to stat2 in the right side of the inequality
    """

    def __init__(self, left, right):
        self.left = left
        self.right = right


class Invariants(ABC):
    """
    This class represent a container for all invariants
    """

    def __init__(self):
        self.invariants = []

    def add(self, invariant):
        self.invariants.append(invariant)


class GreedyTreeInvariants(Invariants):
    """
    Checks the following condition for every invariant:
    change_factor(invariant.left) < change_factor(invariant.right).
    The verification proceeds is in a bottom up order
    """

    def is_invariants_violated(self, new_statistics: StatisticsWrapper, pattern: Pattern):

        if isinstance(new_statistics, SelectivityAndArrivalRatesWrapper):
            (selectivity_matrix, arrival_rates) = new_statistics.statistics
        else:
            raise MissingStatisticsException()

        left_index, right_index = self.invariants[0].left, self.invariants[0].right

        left_change_factor = selectivity_matrix[left_index][left_index] * arrival_rates[left_index]
        right_change_factor = selectivity_matrix[right_index][right_index] * arrival_rates[right_index]
        if left_change_factor > right_change_factor:
            return True

        for i in range(1, len(self.invariants)-1):

            left_change_factor = right_change_factor
            left_change_factor *= selectivity_matrix[right_index][left_index]

            left_index = right_index
            right_index = self.invariants[i].right

            # computes the right change factor
            right_change_factor = selectivity_matrix[right_index][right_index] * arrival_rates[right_index]
            for j in range(0, i):
                right_change_factor *= selectivity_matrix[right_index][self.invariants[j].left]

            if left_change_factor > right_change_factor:
                return True

        return False


class ZStreamTreeInvariants(Invariants):

    def __init__(self, cost_model):
        super().__init__()
        self.__cost_model = cost_model

    def is_invariants_violated(self, new_statistics: StatisticsWrapper, pattern: Pattern):
        """
        Checks the following condition for every invariant:
        cost(invariant.left) < cost(invariant.right).
        """
        for invariant in self.invariants:
            left_cost = self.__cost_model.get_plan_cost(new_statistics, pattern, invariant.left)
            right_cost = self.__cost_model.get_plan_cost(new_statistics, pattern, invariant.right)
            if left_cost > right_cost:
                return True

        return False
