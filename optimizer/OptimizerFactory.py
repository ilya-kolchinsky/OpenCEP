from abc import ABC

from base.Pattern import Pattern
from misc.OptimizerTypes import OptimizerTypes
from misc import DefaultConfig
from misc.StatisticsTypes import StatisticsTypes
from optimizer import Optimizer
from plan.TreePlanBuilderTypes import TreePlanBuilderTypes
from plan.TreePlanBuilderFactory import TreePlanBuilderParameters, TreePlanBuilderFactory


class OptimizerParameters:
    """
    Parameters required for optimizer creation.
    """

    def __init__(self, opt_type: OptimizerTypes = DefaultConfig.DEFAULT_OPTIMIZER_TYPE,
                 tree_plan_params: TreePlanBuilderParameters = TreePlanBuilderParameters()):
        self.type = opt_type
        self.tree_plan_params = tree_plan_params
        # was before in the constructor - pattern


class TrivialOptimizerParameters(OptimizerParameters):
    """
    Parameters for the creation of the trivial optimizer algorithm.
    """

    def __init__(self, opt_type: OptimizerTypes, tree_plan_params: TreePlanBuilderParameters):
        super().__init__(opt_type, tree_plan_params)


class StatisticChangesAwareOptimizerParameters(OptimizerParameters):
    """
    Parameters for the creation of optimizer2 algorithm.
    """

    def __init__(self, opt_type: OptimizerTypes, tree_plan_params: TreePlanBuilderParameters,
                 t: float, stat_type: StatisticsTypes):
        super().__init__(opt_type, tree_plan_params)
        self.t = t
        self.stat_type = stat_type


class InvariantsAwareOptimizerParameters(OptimizerParameters):
    """
    Parameters for the creation of optimizer3 algorithm.
    """

    def __init__(self, opt_type: OptimizerTypes, tree_plan_params: TreePlanBuilderParameters):
        super().__init__(opt_type, tree_plan_params)


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

        if optimizer_parameters.type == OptimizerTypes.CHANGED_BY_T:
            if optimizer_parameters.stat_type == StatisticsTypes.ARRIVAL_RATES:
                return Optimizer.ArrivalRatesChangesAwareOptimizer(tree_plan_builder, optimizer_parameters.t)
            if optimizer_parameters.stat_type == StatisticsTypes.SELECTIVITY_MATRIX:
                return Optimizer.SelectivityChangesAwareOptimizer(tree_plan_builder, optimizer_parameters.t)
            if optimizer_parameters.stat_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
                return Optimizer.SelectivityAndArrivalRatesChangesAwareOptimizer(tree_plan_builder, optimizer_parameters.t)

        if optimizer_parameters.type == OptimizerTypes.USING_INVARIANT:
            return Optimizer.InvariantsAwareOptimizer(tree_plan_builder)

        raise Exception("Unknown optimizer type: %s" % (optimizer_parameters.type,))

    @staticmethod
    def __create_default_optimizer_parameters():
        """
        Uses the default configuration to create optimizer parameters.
        """
        if DefaultConfig.DEFAULT_OPTIMIZER_TYPE == OptimizerTypes.TRIVIAL:
            return OptimizerParameters()

        raise Exception("Unknown optimizer type: %s" % (DefaultConfig.DEFAULT_OPTIMIZER_TYPE,))
