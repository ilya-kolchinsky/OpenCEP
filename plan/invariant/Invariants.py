from abc import ABC, abstractmethod
from collections import Callable

from base.Pattern import Pattern
from misc.LegacyStatistics import MissingStatisticsException
from adaptive.statistics.StatisticsTypes import StatisticsTypes


class Invariant:
    """
    This class represents a single invariant.
    An invariant is defined as an inequality of the following form: f1 (stat1) < f2 (stat2), hence we save the index
     corresponding to stat1 in the left side of the inequality and the index corresponding to stat2 in the right side
     of the inequality.
    """
    def __init__(self, left, right):
        self.left = left
        self.right = right


class Invariants(ABC):
    """
    This class represents a container for all invariants.
    """
    def __init__(self):
        self.invariants = []

    def add(self, invariant):
        self.invariants.append(invariant)

    @abstractmethod
    def is_invariants_violated(self, new_statistics: dict, pattern: Pattern):
        """
        Checks if at least one invariant is violated
        """
        raise NotImplementedError("Must override")


class GreedyTreeInvariants(Invariants):
    """
    Tests the following condition for every invariant in the invariants:
    change_factor(invariant.left) < change_factor(invariant.right).
    The verification proceeds in a bottom up order.
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
    """
    Tests the following condition for every invariant in the invariants:
    cost(invariant.left) < cost(invariant.right).
    """
    def __init__(self, get_plan_cost_callback: Callable):
        super().__init__()
        self.__get_plan_cost_callback = get_plan_cost_callback

    def is_invariants_violated(self, new_statistics: dict, pattern: Pattern):
        for invariant in self.invariants:
            left_cost = self.__get_plan_cost_callback(pattern, invariant.left, new_statistics)
            right_cost = self.__get_plan_cost_callback(pattern, invariant.right, new_statistics)
            if left_cost > right_cost:
                return True
        return False
