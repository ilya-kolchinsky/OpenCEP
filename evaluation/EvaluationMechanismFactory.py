from typing import List

from datetime import timedelta
from base.Pattern import Pattern
from evaluation.EvaluationMechanismTypes import EvaluationMechanismTypes
from misc import DefaultConfig
from tree.TreeEvaluationMechanismTypes import TreeEvaluationMechanismTypes
from optimizer.OptimizerFactory import OptimizerParameters, OptimizerFactory, \
    StatisticsDeviationAwareOptimizerParameters
from statistics_collector.StatisticsCollectorFactory import StatisticsCollectorParameters, StatisticsCollectorFactory
from tree.PatternMatchStorage import TreeStorageParameters
from tree.SimultaneousTreeBasedEvaluationMechanism import SimultaneousTreeBasedEvaluationMechanism
from tree.TrivialTreeBasedEvaluationMechnism import TrivialTreeBasedEvaluationMechanism
from plan.multi.MultiPatternEvaluationParameters import MultiPatternEvaluationParameters


class EvaluationMechanismParameters:
    """
    Parameters required for evaluation mechanism creation.
    """
    def __init__(self, eval_mechanism_type: EvaluationMechanismTypes = DefaultConfig.DEFAULT_EVALUATION_MECHANISM_TYPE,
                 statistics_collector_params: StatisticsCollectorParameters = StatisticsCollectorParameters(),
                 optimizer_params: OptimizerParameters = OptimizerParameters()):
        self.type = eval_mechanism_type
        self.statistics_collector_params = statistics_collector_params
        self.optimizer_params = optimizer_params


class TreeBasedEvaluationMechanismParameters(EvaluationMechanismParameters):
    """
    Parameters for the creation of a tree-based evaluation mechanism.
    """
    def __init__(self,
                 storage_params: TreeStorageParameters = TreeStorageParameters(),
                 multi_pattern_eval_params: MultiPatternEvaluationParameters = MultiPatternEvaluationParameters(),
                 evaluation_type: TreeEvaluationMechanismTypes = DefaultConfig.DEFAULT_TREE_EVALUATION_MECHANISM_TYPE,
                 statistics_collector_params: StatisticsCollectorParameters = StatisticsCollectorParameters(),
                 optimizer_params: OptimizerParameters = StatisticsDeviationAwareOptimizerParameters(),
                 statistics_updates_wait_time: timedelta = DefaultConfig.STATISTICS_UPDATES_WAIT_TIME):
        super().__init__(EvaluationMechanismTypes.TREE_BASED, statistics_collector_params, optimizer_params)
        self.storage_params = storage_params
        self.multi_pattern_eval_params = multi_pattern_eval_params
        self.evaluation_tree_type = evaluation_type
        self.statistics_updates_time_window = statistics_updates_wait_time


class EvaluationMechanismFactory:
    """
    Creates an evaluation mechanism given its specification.
    """

    @staticmethod
    def build_single_pattern_eval_mechanism(eval_mechanism_params: EvaluationMechanismParameters,
                                            pattern: Pattern):
        if eval_mechanism_params is None:
            eval_mechanism_params = EvaluationMechanismFactory.__create_default_eval_parameters()
        if eval_mechanism_params.type == EvaluationMechanismTypes.TREE_BASED:
            return EvaluationMechanismFactory.__create_tree_based_eval_mechanism(eval_mechanism_params, pattern)
        raise Exception("Unknown evaluation mechanism type: %s" % (eval_mechanism_params.type,))

    @staticmethod
    def build_multi_pattern_eval_mechanism(eval_mechanism_params: EvaluationMechanismParameters,
                                           patterns: List[Pattern]):
        if eval_mechanism_params is None:
            eval_mechanism_params = EvaluationMechanismFactory.__create_default_eval_parameters()
        if eval_mechanism_params.type == EvaluationMechanismTypes.TREE_BASED:
            return EvaluationMechanismFactory.__create_tree_based_eval_mechanism(eval_mechanism_params, patterns)
        raise Exception("Unknown evaluation mechanism type: %s" % (eval_mechanism_params.type,))

    @staticmethod
    def __create_tree_based_eval_mechanism(eval_mechanism_params: TreeBasedEvaluationMechanismParameters,
                                           patterns: Pattern or List[Pattern]):
        """
        Instantiates a tree-based CEP evaluation mechanism according to the given configuration.
        """
        if isinstance(patterns, Pattern):
            patterns = [patterns]

        statistics_collector = StatisticsCollectorFactory.build_statistics_collector(eval_mechanism_params.statistics_collector_params, patterns)
        optimizer = OptimizerFactory.build_optimizer(eval_mechanism_params.optimizer_params)
        initial_statistics = statistics_collector.get_statistics()
        cost_model_type = eval_mechanism_params.optimizer_params.tree_plan_params.cost_model_type
        pattern_to_tree_plan_map = {pattern: optimizer.build_initial_tree_plan(initial_statistics, cost_model_type, pattern) for pattern in patterns}

        if eval_mechanism_params.evaluation_tree_type == TreeEvaluationMechanismTypes.TRIVIAL_TREE_EVALUATION:
            return TrivialTreeBasedEvaluationMechanism(pattern_to_tree_plan_map,
                                                       eval_mechanism_params.storage_params,
                                                       statistics_collector,
                                                       optimizer,
                                                       eval_mechanism_params.statistics_updates_time_window,
                                                       eval_mechanism_params.multi_pattern_eval_params)

        if eval_mechanism_params.evaluation_tree_type == TreeEvaluationMechanismTypes.SIMULTANEOUS_TREE_EVALUATION:
            return SimultaneousTreeBasedEvaluationMechanism(pattern_to_tree_plan_map,
                                                            eval_mechanism_params.storage_params,
                                                            statistics_collector,
                                                            optimizer,
                                                            eval_mechanism_params.statistics_updates_time_window,
                                                            eval_mechanism_params.multi_pattern_eval_params)
        raise Exception("Unknown evaluation mechanism type: %s" % (eval_mechanism_params.evaluation_tree_type,))

    @staticmethod
    def __create_default_eval_parameters():
        """
        Uses the default configuration to create evaluation mechanism parameters.
        """
        if DefaultConfig.DEFAULT_EVALUATION_MECHANISM_TYPE == EvaluationMechanismTypes.TREE_BASED:
            return TreeBasedEvaluationMechanismParameters()
        raise Exception("Unknown evaluation mechanism type: %s" % (DefaultConfig.DEFAULT_EVALUATION_MECHANISM_TYPE,))
