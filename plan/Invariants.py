from abc import ABC, abstractmethod

from base.Pattern import Pattern
from misc.Statistics import MissingStatisticsException
from misc.StatisticsTypes import StatisticsTypes
from plan.TreeCostModel import TreeCostModelFactory, TreeCostModel, IntermediateResultsTreeCostModel
from plan.TreeCostModels import TreeCostModels
from statistics_collector.Statistics import Statistics, SelectivityAndArrivalRatesStatistics
from statistics_collector.StatisticsWrapper import StatisticsWrapper, SelectivityAndArrivalRatesWrapper


class Invariant:
    """
    This class represent single invariant.
    invariant will be defined as an inequality of the following form:
    f1 (stat1) < f2 (stat2)
    Hence we save the index corresponding to stat1 in left side of the inequality
    and index corresponding to stat2 in right side of the inequality
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
    Check every invariant in invariants with next condition:
    change_factor(invariant.left) < change_factor(invariant.right).
    The verification proceeds in a bottom up order
    """

    def is_invariants_violated(self, new_statistics: dict, pattern: Pattern):
        if StatisticsTypes.ARRIVAL_RATES in new_statistics and \
                StatisticsTypes.SELECTIVITY_MATRIX in new_statistics and \
                len(new_statistics) == 2:
            selectivity_matrix = new_statistics[StatisticsTypes.SELECTIVITY_MATRIX]
            arrival_rates = new_statistics[StatisticsTypes.ARRIVAL_RATES]
        else:
            raise MissingStatisticsException()

        for i in range(len(self.invariants)-1):

            left_index, right_index = self.invariants[i].left, self.invariants[i].right

            # computes the change factor for both left and right
            left_change_factor = selectivity_matrix[left_index][left_index] * arrival_rates[left_index]
            right_change_factor = selectivity_matrix[right_index][right_index] * arrival_rates[right_index]

            for j in range(i):
                left_change_factor *= selectivity_matrix[left_index][self.invariants[j].left]
                right_change_factor *= selectivity_matrix[right_index][self.invariants[j].left]

            # check if current invariant was violated
            if left_change_factor > right_change_factor:
                return True

        return False


class ZStreamTreeInvariants(Invariants):

    def __init__(self, cost_model):
        super().__init__()
        self.__cost_model = cost_model

    def is_invariants_violated(self, new_statistics: dict, pattern: Pattern):
        """
        Check every invariant in invariants with next condition:
        cost(invariant.left) < cost(invariant.right).
        """
        for invariant in self.invariants:
            left_cost = self.__cost_model.get_plan_cost(new_statistics, pattern, invariant.left)
            right_cost = self.__cost_model.get_plan_cost(new_statistics, pattern, invariant.right)
            if left_cost > right_cost:
                return True

        return False