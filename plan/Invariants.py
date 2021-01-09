from abc import ABC

from base.Pattern import Pattern
from plan.TreeCostModel import TreeCostModelFactory, TreeCostModel
from plan.TreeCostModels import TreeCostModels


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

    def is_invariants_violated(self, pattern: Pattern):

        selectivity_matrix = pattern.statistics[0]
        arrival_rates = pattern.statistics[1]

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
    """
    Check every invariant in invariants with next condition:
    cost(invariant.left) < cost(invariant.right).
    """
    def __init__(self, cost_model: TreeCostModel):
        super().__init__()
        self.__cost_model = cost_model

    def is_invariants_violated(self, pattern: Pattern):

        for (left, right) in self.invariants:
            left_cost = self.__cost_model.get_plan_cost(pattern, left)
            right_cost = self.__cost_model.get_plan_cost(pattern, right)
            if left_cost >= right_cost:
                return True

        return False
