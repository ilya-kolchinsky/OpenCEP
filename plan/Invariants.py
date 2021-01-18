from abc import ABC, abstractmethod

from base.Pattern import Pattern
from misc.Statistics import MissingStatisticsException
from plan.TreeCostModel import TreeCostModelFactory, TreeCostModel, IntermediateResultsTreeCostModel
from plan.TreeCostModels import TreeCostModels
from statistics_collector.NewStatistics import Statistics, SelectivityAndArrivalRatesStatistics
from statistics_collector.StatisticsObjects import StatisticsObject


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

    def is_invariants_violated(self, new_statistics: StatisticsObject, pattern: Pattern):

        if isinstance(new_statistics, SelectivityAndArrivalRatesStatistics):
            (selectivity_matrix, arrival_rates) = new_statistics.get_statistics()
        else:
            raise MissingStatisticsException()

        first_index, second_index = self.invariants[0].left, self.invariants[0].right

        first_change_factor = selectivity_matrix[first_index][first_index] * arrival_rates[first_index]
        second_change_factor = selectivity_matrix[second_index][second_index] * arrival_rates[second_index]
        if first_change_factor > second_change_factor:
            return True

        for i in range(1, len(self.invariants)-1):

            first_change_factor = second_change_factor
            first_change_factor *= selectivity_matrix[second_index][first_index]

            first_index = second_index
            second_index = self.invariants[i].right

            # compute the second change factor
            second_change_factor = selectivity_matrix[second_index][second_index] * arrival_rates[second_index]
            for j in range(0, i):
                second_change_factor *= selectivity_matrix[second_index][self.invariants[j].left]

            if first_change_factor > second_change_factor:
                return True

        return False


class ZStreamTreeInvariants(Invariants):

    def __init__(self, cost_model):
        super().__init__()
        self.__cost_model = cost_model

    def is_invariants_violated(self, new_statistics: StatisticsObject, pattern: Pattern):
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