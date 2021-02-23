from typing import List
from misc.OptimizerTypes import OptimizerTypes
from misc import DefaultConfig
from misc.StatisticsTypes import StatisticsTypes
from optimizer import Optimizer, ChangesAwareTester, ChangesAwareFactory
from plan.InvariantLeftDeepTreeBuilder import InvariantAwareGreedyTreeBuilder
from plan.InvariantTreePlanBuilder import InvariantTreePlanBuilder
from plan.TreePlanBuilderFactory import TreePlanBuilderParameters, TreePlanBuilderFactory
from plan.TreePlanBuilderTypes import TreePlanBuilderTypes


class OptimizerParameters:
    """
    Parameters required for optimizer creation.
    """

    def __init__(self, opt_type: OptimizerTypes = DefaultConfig.DEFAULT_OPTIMIZER_TYPE,
                 tree_plan_params: TreePlanBuilderParameters = TreePlanBuilderParameters()):
        self.type = opt_type
        self.tree_plan_params = tree_plan_params


class TrivialOptimizerParameters(OptimizerParameters):
    """
    Parameters for the creation of the trivial optimizer class.
    """

    def __init__(self, tree_plan_params: TreePlanBuilderParameters = TreePlanBuilderParameters()):
        super().__init__(OptimizerTypes.TRIVIAL, tree_plan_params)


class StatisticChangesAwareOptimizerParameters(OptimizerParameters):
    """
    Parameters for the creation of StatisticChangesAwareOptimizer class.
    """

    def __init__(self, tree_plan_params: TreePlanBuilderParameters = TreePlanBuilderParameters(),
                 statistics_types: List[StatisticsTypes] or StatisticsTypes = DefaultConfig.DEFAULT_STATISTICS_TYPE,
                 t: float = DefaultConfig.THRESHOLD):
        super().__init__(OptimizerTypes.CHANGES_AWARE, tree_plan_params)
        if isinstance(statistics_types, StatisticsTypes):
            statistics_types = [statistics_types]
        self.statistics_types = statistics_types
        self.t = t


class InvariantsAwareOptimizerParameters(OptimizerParameters):
    """
    Parameters for the creation of InvariantsAwareOptimizer class.
    """

    def __init__(self, tree_plan_params: TreePlanBuilderParameters = TreePlanBuilderParameters(TreePlanBuilderTypes.INVARIANT_AWARE_GREEDY_LEFT_DEEP_TREE)):
        super().__init__(OptimizerTypes.USING_INVARIANT, tree_plan_params)


class OptimizerFactory:
    """
    Creates an optimizer given its specification.
    """

    @staticmethod
    def build_optimizer(optimizer_parameters: OptimizerParameters):
        if optimizer_parameters is None:
            optimizer_parameters = OptimizerFactory.__create_default_optimizer_parameters()
        return OptimizerFactory.__create_optimizer(optimizer_parameters)

    @staticmethod
    def __create_optimizer(optimizer_parameters: OptimizerParameters):

        tree_plan_builder = TreePlanBuilderFactory.create_tree_plan_builder(optimizer_parameters.tree_plan_params)
        if optimizer_parameters.type == OptimizerTypes.TRIVIAL:
            return Optimizer.TrivialOptimizer(tree_plan_builder)

        if optimizer_parameters.type == OptimizerTypes.CHANGES_AWARE:
            t = optimizer_parameters.t
            type_to_changes_aware_tester_map = {}
            for stat_type in optimizer_parameters.statistics_types:
                changes_aware_tester = ChangesAwareFactory.create_changes_aware_tester(stat_type, t)
                type_to_changes_aware_tester_map[stat_type] = changes_aware_tester

            return Optimizer.StatisticsChangesAwareOptimizer(tree_plan_builder, type_to_changes_aware_tester_map)

        if optimizer_parameters.type == OptimizerTypes.USING_INVARIANT:
            if isinstance(tree_plan_builder, InvariantTreePlanBuilder):
                return Optimizer.InvariantsAwareOptimizer(tree_plan_builder)
            else:
                raise Exception("Tree plan builder must be invariant aware")

    @staticmethod
    def __create_default_optimizer_parameters():
        """
        Uses default configurations to create optimizer parameters.
        """
        if DefaultConfig.DEFAULT_OPTIMIZER_TYPE == OptimizerTypes.TRIVIAL:
            return TrivialOptimizerParameters()
        if DefaultConfig.DEFAULT_OPTIMIZER_TYPE == OptimizerTypes.CHANGES_AWARE:
            return StatisticChangesAwareOptimizerParameters()
        if DefaultConfig.DEFAULT_OPTIMIZER_TYPE == OptimizerTypes.USING_INVARIANT:
            return InvariantsAwareOptimizerParameters()

        raise Exception("Unknown optimizer type: %s" % (DefaultConfig.DEFAULT_OPTIMIZER_TYPE,))
