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
    def run(self, pattern):
        pass


class NaiveOptimizer(Optimizer):
    """
    optimizer with the first algorithm.
    """

    def run(self, pattern: Pattern):
        # Handles even if it is the first time because in any case the build tree is invoke
        tree_plan = self.__tree_plan_builder.build_tree_plan(pattern)
        self.send_to_evaluation_mechanism(tree_plan)


class StatisticChangesAwareOptimizer(Optimizer):
    """
    optimizer with the second algorithm.
    """

    def __init__(self, tree_plan_builder: TreePlanBuilder, t: int):
        super().__init__(tree_plan_builder)
        self.t = t
        self.prev_statistics = None

    def run(self, pattern: Pattern):
        if self.is_greater_then_t(pattern.statistics):
            tree_plan = self.__tree_plan_builder.build_tree_plan(pattern)
            self.send_to_evaluation_mechanism(tree_plan)

    def is_greater_then_t(self, statistics):
        # if its the first time
        if self.prev_statistics is None:
            return True

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

    def run(self, pattern):
        if self.invariants is None or self.invariants.is_invariants_violated(pattern):
            tree_plan, self.invariants = self.__tree_plan_builder.build_tree_plan(pattern)
            self.send_to_evaluation_mechanism(tree_plan)

