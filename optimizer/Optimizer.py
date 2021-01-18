from abc import ABC, abstractmethod
from base import Pattern
from plan import TreePlanBuilder, TreePlan
from misc.StatisticsTypes import StatisticsTypes
from statistics_collector.NewStatistics import Statistics, ArrivalRatesStatistics, SelectivityStatistics,\
    SelectivityAndArrivalRatesStatistics
from statistics_collector.StatisticsObjects import StatisticsObject, ArrivalRates, SelectivityMatrix, \
    SelectivityMatrixAndArrivalRates


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

    def __init__(self, tree_plan_builder: TreePlanBuilder, t: int):
        super().__init__(tree_plan_builder)
        self.t = t
        self.prev_statistics = None

    def is_need_reoptimize(self, new_statistics: StatisticsObject, pattern: Pattern):
        # Handle the case its the first time
        return self.prev_statistics is None or self.is_greater_then_t(new_statistics)

    def build_new_tree_plan(self, new_statistics: StatisticsObject, pattern: Pattern):
        tree_plan = self.tree_plan_builder.build_tree_plan(new_statistics, pattern)
        return tree_plan

    def is_greater_then_t(self, new_statistics: StatisticsObject):
        if isinstance(new_statistics, ArrivalRates):
            for arrival_rate in new_statistics.statistics:
                if self.prev_statistics + self.t * self.prev_statistics < arrival_rate or \
                        self.prev_statistics - self.t * self.prev_statistics > arrival_rate:
                    return True
            return False

        if isinstance(new_statistics, SelectivityMatrix):
            for row in new_statistics.statistics:
                for selectivity in row:
                    if self.prev_statistics + self.t * self.prev_statistics < selectivity or \
                            self.prev_statistics - self.t * self.prev_statistics > selectivity:
                        return True
            return False

        if isinstance(new_statistics, SelectivityMatrixAndArrivalRates):
            (selectivity_matrix, arrival_rates) = new_statistics.statistics
            for arrival_rate in arrival_rates:
                if self.prev_statistics + self.t * self.prev_statistics < arrival_rate or \
                        self.prev_statistics - self.t * self.prev_statistics > arrival_rate:
                    return True

            for row in selectivity_matrix:
                for selectivity in row:
                    if self.prev_statistics + self.t * self.prev_statistics < selectivity or \
                            self.prev_statistics - self.t * self.prev_statistics > selectivity:
                        return True

            return False


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
