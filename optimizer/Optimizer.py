from abc import ABC, abstractmethod
from base import Pattern
from plan import TreePlanBuilder, TreePlan
from statistics_collector.StatisticsCollector import StatisticsCollector
from misc.StatisticsTypes import StatisticsTypes


class Optimizer(ABC):
    """
    An abstract class for optimizer.
    """

    def __init__(self, pattern: Pattern, tree_plan_builder: TreePlanBuilder):
        self.__tree_plan_builder = tree_plan_builder
        self.pattern = pattern

    # def get_new_statistics(self, statistics):
    #     if self.is_need_to_change():
    #         self.change()
    #         # after change send, check if need to send

    def send_to_evaluation_mechanism(self, tree_plan: TreePlan):
        pass

    @abstractmethod
    def run(self, statistics):
        pass

    # def change(self):
    #     self.__tree_plan_builder.build_tree_plan()


class Optimizer1(Optimizer):
    """
    optimizer with the first algorithm.
    """

    def run(self, statistics):
        # handle the case its first time because anyway build tree is invoke
        tree_plan = self.__tree_plan_builder.build_tree_plan(self.pattern)
        self.send_to_evaluation_mechanism(tree_plan)


class Optimizer2(Optimizer):
    """
    optimizer with the second algorithm.
    """

    def __init__(self, pattern, tree_plan_builder: TreePlanBuilder, t: int):
        super().__init__(pattern, tree_plan_builder)
        self.t = t
        self.prev_statistics = None

    def run(self, statistics):
        if self.is_greater_then_t(statistics):
            tree_plan = self.__tree_plan_builder.build_tree_plan(self.pattern)
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


class Optimizer3(Optimizer):
    """
    optimizer with the third algorithm.
    """

    def __init__(self, tree_plan_builder: TreePlanBuilder, pattern):
        super().__init__(tree_plan_builder, pattern)
        self.current_conditions = None

    def run(self, statistics):
        if self.is_condition_violated(statistics):
            tree_plan = self.__tree_plan_builder.build_tree_plan(self.pattern)
            Optimizer3.calculate_new_conditions(tree_plan)
            self.send_to_evaluation_mechanism(tree_plan)

    """
    The functions below are adapted to each algorithm individually.
    Hence any class that inherits from this class should implement these functions
    """
    @abstractmethod
    def is_condition_violated(self, statistics):
        pass

    @abstractmethod
    def calculate_new_conditions(self):
        pass


class TrivialLeftDeepTreeOptimizer(Optimizer3):
    """
    Creates an optimizer with the third algorithm corresponding to the LeftDeepTreeBuilders.
    """

    def is_condition_violated(self, statistics):
        pass

    def calculate_new_conditions(self):
        pass


class AscendingFrequencyTreeOptimizer(Optimizer3):
    """
    Creates an optimizer with the third algorithm corresponding to the DynamicProgrammingBushyTreeBuilder.
    """

    def is_condition_violated(self, statistics):
        pass

    def calculate_new_conditions(self):
        pass


class GreedyLeftDeepTreeOptimizer(Optimizer3):
    """
    Creates an optimizer with the third algorithm corresponding to the DynamicProgrammingBushyTreeBuilder.
    """

    def is_condition_violated(self, statistics):
        pass

    def calculate_new_conditions(self):
        pass


class IterativeImprovementLeftDeepTreeOptimizer(Optimizer3):
    """
    Creates an optimizer with the third algorithm corresponding to the DynamicProgrammingBushyTreeBuilder.
    """

    def is_condition_violated(self, statistics):
        pass

    def calculate_new_conditions(self):
        pass


class DynamicProgrammingLeftDeepTreeOptimizer(Optimizer3):
    """
    Creates an optimizer with the third algorithm corresponding to the DynamicProgrammingBushyTreeBuilder.
    """

    def is_condition_violated(self, statistics):
        pass

    def calculate_new_conditions(self):
        pass


class DynamicProgrammingBushyTreeOptimizer(Optimizer3):
    """
    Creates an optimizer with the third algorithm corresponding to the DynamicProgrammingBushyTreeBuilder.
    """

    def is_condition_violated(self, statistics):
        pass

    def calculate_new_conditions(self):
        pass


class ZStreamTreeOptimizer(Optimizer3):
    """
    Creates an optimizer with the third algorithm corresponding to the DynamicProgrammingBushyTreeBuilder.
    """

    def is_condition_violated(self, statistics):
        pass

    def calculate_new_conditions(self):
        pass


class ZStreamOrdTreeOptimizer(Optimizer3):
    """
    Creates an optimizer with the third algorithm corresponding to the DynamicProgrammingBushyTreeBuilder.
    """

    def is_condition_violated(self, statistics):
        pass

    def calculate_new_conditions(self):
        pass
