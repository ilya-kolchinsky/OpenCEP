from abc import ABC, abstractmethod
from base import Pattern
from misc import DefaultConfig
from plan import TreePlanBuilder, TreePlan
from plan.LeftDeepTreeBuilders import TrivialLeftDeepTreeBuilder
from plan.TreePlanBuilderTypes import TreePlanBuilderTypes
from statistics_collector.StatisticsWrapper import StatisticsWrapper


class Optimizer(ABC):
    """
    An abstract class for optimizer.
    """

    def __init__(self, tree_plan_builder: TreePlanBuilder):
        self._tree_plan_builder = tree_plan_builder

    @abstractmethod
    def is_need_optimize(self, new_statistics: StatisticsWrapper, pattern: Pattern):
        """
        Asks if it's necessary to optimize the tree based on the new statistics.
        """
        raise NotImplementedError()

    @abstractmethod
    def build_new_tree_plan(self, new_statistics: StatisticsWrapper, pattern: Pattern):
        raise NotImplementedError()

    def build_initial_tree_plan(self, new_statistics: StatisticsWrapper, pattern: Pattern):

        if pattern.statistics is None:
            if DefaultConfig.DEFAULT_TREE_PLAN_BUILDER == TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE:
                cost_model = self._tree_plan_builder.__cost_model
                tree_plan_builder = TrivialLeftDeepTreeBuilder(cost_model)
                temp_tree_plan_builder, self._tree_plan_builder = self._tree_plan_builder, tree_plan_builder
                tree_plan = self._tree_plan_builder.build_tree_plan(new_statistics, pattern)
                self.internal_init()
                return tree_plan
            else:
                raise Exception("Unknown tree plan builder type: %s" % (DefaultConfig.DEFAULT_TREE_PLAN_BUILDER,))
        else:
            tree_plan = self.build_new_tree_plan(new_statistics, pattern)
            return tree_plan


class TrivialOptimizer(Optimizer):
    """
    Represents the trivial optimizer that always says to re-optimize the tree, ignoring the statistics.
    """

    def is_need_optimize(self, new_statistics: StatisticsWrapper, pattern: Pattern):
        return True

    def build_new_tree_plan(self, new_statistics: StatisticsWrapper, pattern: Pattern):
        tree_plan = self._tree_plan_builder.build_tree_plan(new_statistics, pattern)
        return tree_plan


class StatisticChangesAwareOptimizer(Optimizer):
    """
    Represents the optimizer that is aware of the changes in the statistics,
    i.e if one of the statistics is changed by constant t then the optimizer will say that tree optimization is needed.
    """

    def __init__(self, tree_plan_builder: TreePlanBuilder, t):
        super().__init__(tree_plan_builder)
        self._t = t
        self._prev_statistics = None

    def is_need_optimize(self, new_statistics: StatisticsWrapper, pattern: Pattern):
        return self._prev_statistics is None or self.is_changed_by_t(new_statistics.statistics, self._prev_statistics)

    def build_new_tree_plan(self, new_statistics: StatisticsWrapper, pattern: Pattern):
        self._prev_statistics = new_statistics.statistics
        tree_plan = self._tree_plan_builder.build_tree_plan(new_statistics, pattern)
        return tree_plan

    def is_changed_by_t(self, new_statistics, old_statistics):
        """
        Checks if there was a changes in one of the statistics by a factor of t.
        """
        raise NotImplementedError()


class ArrivalRatesChangesAwareOptimizer(StatisticChangesAwareOptimizer):
    """
    Checks for changes in the arrival rate by a factor of t.
    """

    def is_changed_by_t(self, new_arrival_rates, old_arrival_rates):

        for i in range(len(new_arrival_rates)):
            if old_arrival_rates[i] * (1 + self._t) < new_arrival_rates[i] or \
                    old_arrival_rates[i] * (1 - self._t) > new_arrival_rates[i]:
                return True
        return False


class SelectivityChangesAwareOptimizer(StatisticChangesAwareOptimizer):
    """
    Checks for changes in the selectivity by a factor of t.
    """

    def is_changed_by_t(self, new_selectivity, old_selectivity):

        for i in range(len(new_selectivity)):
            for j in range(len(new_selectivity[i])):
                if old_selectivity[i][j] * (1 + self._t) < new_selectivity[i][j] or \
                        old_selectivity[i][j] * (1 - self._t) > new_selectivity[i][j]:
                    return True
        return False


class SelectivityAndArrivalRatesChangesAwareOptimizer(ArrivalRatesChangesAwareOptimizer,
                                                      SelectivityChangesAwareOptimizer):
    """
    Checks for changes in both arrival rates and selectivity by a factor of t.
    """

    def is_changed_by_t(self, new_statistics, old_statistics):
        (new_selectivity, new_arrival_rates) = new_statistics
        (old_selectivity, old_arrival_rates) = old_statistics
        return ArrivalRatesChangesAwareOptimizer.is_changed_by_t(self, new_arrival_rates, old_arrival_rates) or \
               SelectivityChangesAwareOptimizer.is_changed_by_t(self, new_selectivity, old_selectivity)


class InvariantsAwareOptimizer(Optimizer):
    """
    Represents the invariants aware optimizer that checks if at least one invariant was violated.
    """

    def __init__(self, tree_plan_builder: TreePlanBuilder):
        super().__init__(tree_plan_builder)
        self._invariants = None

    def is_need_optimize(self, new_statistics: StatisticsWrapper, pattern: Pattern):
        return self._invariants is None or self._invariants.is_invariants_violated(new_statistics, pattern)

    def build_new_tree_plan(self, new_statistics: StatisticsWrapper, pattern: Pattern):
        tree_plan, self._invariants = self._tree_plan_builder.build_tree_plan(new_statistics, pattern)
        return tree_plan

