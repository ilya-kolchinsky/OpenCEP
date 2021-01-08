from abc import ABC, abstractmethod
from base import Pattern
from plan import TreePlanBuilder, TreePlan
from plan.IterativeImprovement import IterativeImprovementInitType
from statistics_collector.StatisticsCollector import StatisticsCollector
from misc.StatisticsTypes import StatisticsTypes


class Optimizer(ABC):
    """
    An abstract class for optimizer.
    """

    def __init__(self, tree_plan_builder: TreePlanBuilder):
        self.__tree_plan_builder = tree_plan_builder

    def send_to_evaluation_mechanism(self, tree_plan: TreePlan):
        pass

    @abstractmethod
    def is_need_reoptimize(self, pattern):
        pass

    @abstractmethod
    def build_new_tree_plan(self, pattern: Pattern):
        pass


class NaiveOptimizer(Optimizer):
    """
    optimizer with the first algorithm.
    """

    def is_need_reoptimize(self, pattern: Pattern):
        return True

    def build_new_tree_plan(self, pattern: Pattern):
        tree_plan = self.__tree_plan_builder.build_tree_plan(pattern)
        return tree_plan


class StatisticChangesAwareOptimizer(Optimizer):
    """
    optimizer with the second algorithm.
    """

    def __init__(self, tree_plan_builder: TreePlanBuilder, t: int):
        super().__init__(tree_plan_builder)
        self.t = t
        self.prev_statistics = None

    def is_need_reoptimize(self, pattern: Pattern):
        # Handle the case its the first time
        return self.prev_statistics is None or self.is_greater_then_t(pattern.statistics)

    def build_new_tree_plan(self, pattern: Pattern):
        tree_plan = self.__tree_plan_builder.build_tree_plan(pattern)
        return tree_plan

    @staticmethod
    def is_greater_then_t(statistics):
        if statistics.statistics_type == StatisticsTypes.ARRIVAL_RATES:
            pass
        if statistics.statistics_type == StatisticsTypes.SELECTIVITY_MATRIX:
            pass
        if statistics.statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            pass


class InvariantAwareOptimizer(Optimizer):
    """
    optimizer with the third algorithm.
    """

    def __init__(self, tree_plan_builder: TreePlanBuilder):
        super().__init__(tree_plan_builder)
        self.invariants = None

    def is_need_reoptimize(self, pattern):
        # Handle the case its the first time
        return self.invariants is None or self.invariants.is_invariants_violated(pattern)

    def build_new_tree_plan(self, pattern: Pattern):
        tree_plan, self.invariants = self.__tree_plan_builder.build_tree_plan(pattern)
        return tree_plan
