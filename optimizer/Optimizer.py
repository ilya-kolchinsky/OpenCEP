from abc import ABC, abstractmethod
from base import Pattern
from misc.Statistics import MissingStatisticsException
from plan import TreePlanBuilder, TreePlan
from misc.StatisticsTypes import StatisticsTypes
from statistics_collector.NewStatistics import Statistics, ArrivalRatesStatistics, SelectivityStatistics, \
    SelectivityAndArrivalRatesStatistics
from statistics_collector.StatisticsObjects import StatisticsObject, ArrivalRates, SelectivityMatrix, \
    SelectivityMatrixAndArrivalRates
import copy

class Optimizer(ABC):
    """
    An abstract class for optimizer.
    """

    def __init__(self, tree_plan_builder: TreePlanBuilder):
        self.tree_plan_builder = tree_plan_builder

    def send_to_evaluation_mechanism(self, tree_plan: TreePlan):
        pass

    @abstractmethod
    def is_need_reoptimize(self, new_statistics: StatisticsObject, pattern: Pattern):
        pass

    @abstractmethod
    def build_new_tree_plan(self, new_statistics: StatisticsObject, pattern: Pattern):
        pass


class NaiveOptimizer(Optimizer):
    """
    optimizer with the first algorithm.
    """

    def is_need_reoptimize(self, new_statistics: StatisticsObject, pattern: Pattern):
        return True

    def build_new_tree_plan(self, new_statistics: StatisticsObject, pattern: Pattern):
        tree_plan = self.tree_plan_builder.build_tree_plan(new_statistics, pattern)
        return tree_plan


class StatisticChangesAwareOptimizer(Optimizer):
    """
    optimizer with the second algorithm.
    """

    def __init__(self, tree_plan_builder: TreePlanBuilder, t):
        super().__init__(tree_plan_builder)
        self.t = t
        self.prev_statistics = None

    def is_need_reoptimize(self, new_statistics: StatisticsObject, pattern: Pattern):
        # Handle the case its the first time
        return self.prev_statistics is None or self.is_changed_by_t(new_statistics.statistics, self.prev_statistics)

    def build_new_tree_plan(self, new_statistics: StatisticsObject, pattern: Pattern):
        self.prev_statistics = copy.deepcopy(new_statistics.statistics)
        # print(self.prev_statistics)
        tree_plan = self.tree_plan_builder.build_tree_plan(new_statistics, pattern)
        return tree_plan

    def is_changed_by_t(self, new_statistics, old_statistics):
        pass


#     def is_changed_by_t(self, new_statistics: StatisticsObject):
#         if isinstance(new_statistics, ArrivalRates):
#             arrival_rates = new_statistics.statistics
#             for i in range(len(arrival_rates)):
#                 if self.prev_statistics[i] + self.t * self.prev_statistics[i] < arrival_rates[i] or \
#                         self.prev_statistics[i] - self.t * self.prev_statistics[i] > arrival_rates[i]:
#                     return True
#
#             return False
#
#         if isinstance(new_statistics, SelectivityMatrix):
#             selectivity = new_statistics.statistics
#             for i in range(len(selectivity)):
#                 for j in range(len(selectivity[i])):
#                     if self.prev_statistics[i][j] + self.t * self.prev_statistics[i][j] < selectivity[i][j] or \
#                             self.prev_statistics[i][j] - self.t * self.prev_statistics[i][j] > selectivity[i][j]:
#                         return True
#             return False
#
#         if isinstance(new_statistics, SelectivityMatrixAndArrivalRates):
#             (selectivity, arrival_rates) = new_statistics.statistics
#             for i in range(len(arrival_rates)):
#                 if self.prev_statistics[i] + self.t * self.prev_statistics[i] < arrival_rates[i] or \
#                         self.prev_statistics[i] - self.t * self.prev_statistics[i] > arrival_rates[i]:
#                     return True
#
#             for i in range(len(selectivity)):
#                 for j in range(len(selectivity[i])):
#                     if self.prev_statistics[i][j] + self.t * self.prev_statistics[i][j] < selectivity[i][j] or \
#                             self.prev_statistics[i][j] - self.t * self.prev_statistics[i][j] > selectivity[i][j]:
#                         return True
#
#             return False


class ArrivalStatisticChangesAwareOptimizer(StatisticChangesAwareOptimizer):

    def is_changed_by_t(self, new_arrival_rates, old_arrival_rates):

        for i in range(len(new_arrival_rates)):
            if old_arrival_rates[i] + self.t * old_arrival_rates[i] < new_arrival_rates[i] or \
                    old_arrival_rates[i] - self.t * old_arrival_rates[i] > new_arrival_rates[i]:
                return True
        return False


class SelectivityStatisticChangesAwareOptimizer(StatisticChangesAwareOptimizer):

    def is_changed_by_t(self, new_selectivity, old_selectivity):

        for i in range(len(new_selectivity)):
            for j in range(len(new_selectivity[i])):
                if old_selectivity[i][j] + self.t * old_selectivity[i][j] < new_selectivity[i][j] or \
                        old_selectivity[i][j] - self.t * old_selectivity[i][j] > new_selectivity[i][j]:
                    return True
        return False


class SelectivityAndArrivalStatisticChangesAwareOptimizer(ArrivalStatisticChangesAwareOptimizer,
                                                          SelectivityStatisticChangesAwareOptimizer):

    def is_changed_by_t(self, new_statistics, old_statistics):
        (new_selectivity, new_arrival_rates) = new_statistics
        (old_selectivity, old_arrival_rates) = old_statistics
        return ArrivalStatisticChangesAwareOptimizer.is_changed_by_t(self, new_arrival_rates, old_arrival_rates) or \
               SelectivityStatisticChangesAwareOptimizer.is_changed_by_t(self, new_selectivity, old_selectivity)


class InvariantAwareOptimizer(Optimizer):
    """
    optimizer with the third algorithm.
    """

    def __init__(self, tree_plan_builder: TreePlanBuilder):
        super().__init__(tree_plan_builder)
        self.invariants = None

    def is_need_reoptimize(self, new_statistics: StatisticsObject, pattern: Pattern):
        # Handle the case its the first time
        return self.invariants is None or self.invariants.is_invariants_violated(new_statistics, pattern)

    def build_new_tree_plan(self, new_statistics: StatisticsObject, pattern: Pattern):
        tree_plan, self.invariants = self.tree_plan_builder.build_tree_plan(new_statistics, pattern)
        return tree_plan
