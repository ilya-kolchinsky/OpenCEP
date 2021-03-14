from datetime import timedelta
from adaptive.optimizer.OptimizerTypes import OptimizerTypes
from adaptive.statistics.StatisticsCollectorFactory import StatisticsCollectorParameters
from misc import DefaultConfig
from adaptive.statistics.StatisticsTypes import StatisticsTypes
from adaptive.optimizer import Optimizer
from adaptive.optimizer.DeviationAwareTesterFactory import DeviationAwareTesterFactory
from plan.invariant.InvariantTreePlanBuilder import InvariantTreePlanBuilder
from plan.TreePlanBuilderFactory import TreePlanBuilderParameters, TreePlanBuilderFactory
from plan.TreePlanBuilderTypes import TreePlanBuilderTypes


class OptimizerParameters:
    """
    Parameters required for optimizer creation.
    """
    def __init__(self, opt_type: OptimizerTypes = DefaultConfig.DEFAULT_OPTIMIZER_TYPE,
                 tree_plan_params: TreePlanBuilderParameters = TreePlanBuilderParameters(),
                 statistics_collector_params: StatisticsCollectorParameters = StatisticsCollectorParameters(),
                 statistics_updates_wait_time: timedelta = DefaultConfig.STATISTICS_UPDATES_WAIT_TIME):
        self.type = opt_type
        self.tree_plan_params = tree_plan_params
        self.statistics_collector_params = statistics_collector_params
        self.statistics_updates_time_window = statistics_updates_wait_time  # None disabled any adaptive functionality


class TrivialOptimizerParameters(OptimizerParameters):
    """
    Parameters for the creation of the trivial optimizer class.
    """
    def __init__(self, tree_plan_params: TreePlanBuilderParameters = TreePlanBuilderParameters(),
                 statistics_collector_params: StatisticsCollectorParameters = StatisticsCollectorParameters(),
                 statistics_updates_wait_time: timedelta = DefaultConfig.STATISTICS_UPDATES_WAIT_TIME):
        super().__init__(OptimizerTypes.TRIVIAL_OPTIMIZER, tree_plan_params,
                         statistics_collector_params, statistics_updates_wait_time)


class StatisticsDeviationAwareOptimizerParameters(OptimizerParameters):
    """
    Parameters for the creation of StatisticDeviationAwareOptimizer class.
    """
    def __init__(self, tree_plan_params: TreePlanBuilderParameters = TreePlanBuilderParameters(),
                 statistics_collector_params: StatisticsCollectorParameters = StatisticsCollectorParameters(),
                 statistics_updates_wait_time: timedelta = DefaultConfig.STATISTICS_UPDATES_WAIT_TIME,
                 deviation_threshold: float = DefaultConfig.DEVIATION_OPTIMIZER_THRESHOLD):
        super().__init__(OptimizerTypes.STATISTICS_DEVIATION_AWARE_OPTIMIZER, tree_plan_params,
                         statistics_collector_params, statistics_updates_wait_time)
        statistics_types = statistics_collector_params.statistics_types
        if isinstance(statistics_types, StatisticsTypes):
            statistics_types = [statistics_types]
        self.statistics_types = statistics_types
        self.deviation_threshold = deviation_threshold


class InvariantsAwareOptimizerParameters(OptimizerParameters):
    """
    Parameters for the creation of InvariantsAwareOptimizer class.
    """
    def __init__(self, tree_plan_params: TreePlanBuilderParameters = TreePlanBuilderParameters(TreePlanBuilderTypes.INVARIANT_AWARE_GREEDY_LEFT_DEEP_TREE),
                 statistics_collector_params: StatisticsCollectorParameters = StatisticsCollectorParameters(),
                 statistics_updates_wait_time: timedelta = DefaultConfig.STATISTICS_UPDATES_WAIT_TIME):
        super().__init__(OptimizerTypes.INVARIANT_AWARE_OPTIMIZER, tree_plan_params,
                         statistics_collector_params, statistics_updates_wait_time)


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
        is_adaptivity_enabled = optimizer_parameters.statistics_updates_time_window is not None
        if optimizer_parameters.type == OptimizerTypes.TRIVIAL_OPTIMIZER:
            return Optimizer.TrivialOptimizer(tree_plan_builder, is_adaptivity_enabled)

        if optimizer_parameters.type == OptimizerTypes.STATISTICS_DEVIATION_AWARE_OPTIMIZER:
            deviation_threshold = optimizer_parameters.deviation_threshold
            type_to_deviation_aware_tester_map = {}
            for stat_type in optimizer_parameters.statistics_types:
                deviation_aware_tester = DeviationAwareTesterFactory.create_deviation_aware_tester(stat_type,
                                                                                                   deviation_threshold)
                type_to_deviation_aware_tester_map[stat_type] = deviation_aware_tester

            return Optimizer.StatisticsDeviationAwareOptimizer(tree_plan_builder, is_adaptivity_enabled,
                                                               type_to_deviation_aware_tester_map)

        if optimizer_parameters.type == OptimizerTypes.INVARIANT_AWARE_OPTIMIZER:
            if isinstance(tree_plan_builder, InvariantTreePlanBuilder):
                return Optimizer.InvariantsAwareOptimizer(tree_plan_builder, is_adaptivity_enabled)
            else:
                raise Exception("Tree plan builder must be invariant aware")

        raise Exception("Unknown optimizer type specified")

    @staticmethod
    def __create_default_optimizer_parameters():
        """
        Uses default configurations to create optimizer parameters.
        """
        if DefaultConfig.DEFAULT_OPTIMIZER_TYPE == OptimizerTypes.TRIVIAL_OPTIMIZER:
            return TrivialOptimizerParameters()
        if DefaultConfig.DEFAULT_OPTIMIZER_TYPE == OptimizerTypes.STATISTICS_DEVIATION_AWARE_OPTIMIZER:
            return StatisticsDeviationAwareOptimizerParameters()
        if DefaultConfig.DEFAULT_OPTIMIZER_TYPE == OptimizerTypes.INVARIANT_AWARE_OPTIMIZER:
            return InvariantsAwareOptimizerParameters()
        raise Exception("Unknown optimizer type: %s" % (DefaultConfig.DEFAULT_OPTIMIZER_TYPE,))
