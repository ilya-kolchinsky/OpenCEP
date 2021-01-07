from abc import ABC

from base.Pattern import Pattern
from misc.OptimizerTypes import OptimizerTypes
from misc import DefaultConfig
from optimizer import Optimizer
from plan.TreePlanBuilderTypes import TreePlanBuilderTypes
from plan.TreePlanBuilderFactory import TreePlanBuilderParameters, TreePlanBuilderFactory


class OptimizerParameters:
    """
    Parameters required for optimizer creation.
    """

    def __init__(self, pattern: Pattern = None,
                 opt_type: OptimizerTypes = DefaultConfig.DEFAULT_OPTIMIZER_TYPE,
                 tree_plan_params: TreePlanBuilderParameters = TreePlanBuilderParameters()):
        self.pattern = pattern
        self.type = opt_type
        self.tree_plan_params = tree_plan_params


class Optimizer1Parameters(OptimizerParameters):
    """
    Parameters for the creation of optimizer1 algorithm.
    """

    def __init__(self, pattern: Pattern, opt_type: OptimizerTypes, tree_plan_params: TreePlanBuilderParameters):
        super().__init__(pattern, opt_type, tree_plan_params)


class Optimizer2Parameters(OptimizerParameters):
    """
    Parameters for the creation of optimizer2 algorithm.
    """

    def __init__(self, pattern: Pattern, opt_type: OptimizerTypes, tree_plan_params: TreePlanBuilderParameters,
                 t: int):
        super().__init__(pattern, opt_type, tree_plan_params)
        self.t = t


class Optimizer3Parameters(OptimizerParameters):
    """
    Parameters for the creation of optimizer3 algorithm.
    """

    def __init__(self, pattern: Pattern, opt_type: OptimizerTypes, tree_plan_params: TreePlanBuilderParameters,
                 current_plan):
        super().__init__(pattern, opt_type, tree_plan_params)
        self.current_plan = current_plan


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
            return Optimizer.NaiveOptimizer(tree_plan_builder)

        if optimizer_parameters.type == OptimizerTypes.GREATER_THEN_T:
            return Optimizer.StatisticChangesAwareOptimizer(tree_plan_builder, optimizer_parameters.t)

        if optimizer_parameters.type == OptimizerTypes.USING_INVARIANT:
            return Optimizer.InvariantAwareOptimizer(tree_plan_builder)

        raise Exception("Unknown optimizer type: %s" % (optimizer_parameters.type,))

    """
    @staticmethod
    def __create_optimizer(optimizer_parameters: OptimizerParameters):

        tree_plan_builder = TreePlanBuilderFactory.create_tree_plan_builder(optimizer_parameters.tree_plan_params)
        if optimizer_parameters.type == OptimizerTypes.TRIVIAL:
            return Optimizer.Optimizer1(optimizer_parameters.pattern, tree_plan_builder)

        if optimizer_parameters.type == OptimizerTypes.GREATER_THEN_T:
            return Optimizer.Optimizer2(optimizer_parameters.pattern,
                                        tree_plan_builder,
                                        optimizer_parameters.t)
        if optimizer_parameters.type == OptimizerTypes.USING_INVARIANT:
            return Optimizer.InvariantAwareOptimizer(optimizer_parameters.pattern, tree_plan_builder)

        raise Exception("Unknown optimizer type: %s" % (optimizer_parameters.type,))
    """
    @staticmethod
    def __create_default_optimizer_parameters():
        """
        Uses the default configuration to create optimizer parameters.
        """
        if DefaultConfig.DEFAULT_OPTIMIZER_TYPE == OptimizerTypes.TRIVIAL:
            return OptimizerParameters()

        raise Exception("Unknown optimizer type: %s" % (DefaultConfig.DEFAULT_OPTIMIZER_TYPE,))
    """
    @staticmethod
    def create_optimizer_3(optimizer_parameters: OptimizerParameters, tree_plan_builder):

        if optimizer_parameters.tree_plan_params.builder_type == TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE:
            return Optimizer.TrivialLeftDeepTreeOptimizer(tree_plan_builder, optimizer_parameters.pattern)
        if optimizer_parameters.tree_plan_params.builder_type == TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE:
            return Optimizer.AscendingFrequencyTreeOptimizer(tree_plan_builder, optimizer_parameters.pattern)
        if optimizer_parameters.tree_plan_params.builder_type == TreePlanBuilderTypes.GREEDY_LEFT_DEEP_TREE:
            return Optimizer.GreedyLeftDeepTreeOptimizer(tree_plan_builder, optimizer_parameters.pattern)
        if optimizer_parameters.tree_plan_params.builder_type == TreePlanBuilderTypes.LOCAL_SEARCH_LEFT_DEEP_TREE:
            return Optimizer.IterativeImprovementLeftDeepTreeOptimizer(tree_plan_builder, optimizer_parameters.pattern)
        if optimizer_parameters.tree_plan_params.builder_type == TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE:
            return Optimizer.DynamicProgrammingLeftDeepTreeOptimizer(tree_plan_builder, optimizer_parameters.pattern)
        if optimizer_parameters.tree_plan_params.builder_type == TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE:
            return Optimizer.DynamicProgrammingBushyTreeOptimizer(tree_plan_builder, optimizer_parameters.pattern)
        if optimizer_parameters.tree_plan_params.builder_type == TreePlanBuilderTypes.ZSTREAM_BUSHY_TREE:
            return Optimizer.ZStreamTreeOptimizer(tree_plan_builder, optimizer_parameters.pattern)
        if optimizer_parameters.tree_plan_params.builder_type == TreePlanBuilderTypes.ORDERED_ZSTREAM_BUSHY_TREE:
            return Optimizer.ZStreamOrdTreeOptimizer(tree_plan_builder, optimizer_parameters.pattern)
        raise Exception("Unknown optimizer type: %s" % (tree_plan_builder.builder_type,))
    """

    """
    @staticmethod
    def create_optimizer_3(optimizer_parameters: OptimizerParameters, tree_plan_builder):

        if optimizer_parameters.tree_plan_params.builder_type == TreePlanBuilderTypes.GREEDY_LEFT_DEEP_TREE:
            return Optimizer.InvariantawareGreedyTreeBuilder(tree_plan_builder, optimizer_parameters.pattern)
        if optimizer_parameters.tree_plan_params.builder_type == TreePlanBuilderTypes.ZSTREAM_BUSHY_TREE:
            return Optimizer.InvariantawareZstreamTreeBuilder(tree_plan_builder, optimizer_parameters.pattern)
        raise Exception("Unknown optimizer type: %s" % (tree_plan_builder.builder_type,))

    """
