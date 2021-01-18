from datetime import timedelta
from typing import List
from base.Pattern import Pattern
from evaluation.EvaluationMechanismTypes import EvaluationMechanismTypes
from misc import DefaultConfig
from misc.Tree_Evaluation_Mechanism_Types import TreeEvaluationMechanismTypes
from plan.TreePlanBuilderFactory import TreePlanBuilderParameters, TreePlanBuilderFactory
from tree.PatternMatchStorage import TreeStorageParameters
from tree.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism, TrivialEvaluation, SimultaneousEvaluation
from plan.multi.MultiPatternEvaluationParameters import MultiPatternEvaluationParameters
from statistics_collector.StatisticsCollector import StatisticsCollector
from optimizer import Optimizer


class EvaluationMechanismParameters:
    """
    Parameters required for evaluation mechanism creation.
    """
    def __init__(self, eval_mechanism_type: EvaluationMechanismTypes = DefaultConfig.DEFAULT_EVALUATION_MECHANISM_TYPE):
        self.type = eval_mechanism_type


class TreeBasedEvaluationMechanismParameters(EvaluationMechanismParameters):
    """
    Parameters for the creation of a tree-based evaluation mechanism.
    """
    def __init__(self, statistics_updates_time_window: timedelta = timedelta(seconds=30),
                 tree_plan_params: TreePlanBuilderParameters = TreePlanBuilderParameters(),
                 storage_params: TreeStorageParameters = TreeStorageParameters(),
                 multi_pattern_eval_params: MultiPatternEvaluationParameters = MultiPatternEvaluationParameters(),
                 evaluation_type: TreeEvaluationMechanismTypes = DefaultConfig.DEFAULT_TREE_EVALUATION_MECHANISM_TYPE):
        super().__init__(EvaluationMechanismTypes.TREE_BASED)
        self.statistics_updates_time_window = statistics_updates_time_window
        self.tree_plan_params = tree_plan_params
        self.storage_params = storage_params
        self.multi_pattern_eval_params = multi_pattern_eval_params
        self.evaluation_type = evaluation_type


class EvaluationMechanismFactory:
    """
    Creates an evaluation mechanism given its specification.
    """

    @staticmethod
    def build_single_pattern_eval_mechanism(eval_mechanism_params: EvaluationMechanismParameters,
                                            pattern: Pattern, statistics_collector: StatisticsCollector,
                                            optimizer: Optimizer):
        if eval_mechanism_params is None:
            eval_mechanism_params = EvaluationMechanismFactory.__create_default_eval_parameters()
        if eval_mechanism_params.type == EvaluationMechanismTypes.TREE_BASED:
            return EvaluationMechanismFactory.__create_tree_based_eval_mechanism(eval_mechanism_params, pattern,
                                                                                 statistics_collector,
                                                                                 optimizer)
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
                                           patterns: Pattern or List[Pattern], statistics_collector: StatisticsCollector,
                                           optimizer: Optimizer):
        """
        Instantiates a tree-based CEP evaluation mechanism according to the given configuration.
        """
        if isinstance(patterns, Pattern):
            patterns = [patterns]

        # This row is for create the tree builder
        tree_plan_builder = TreePlanBuilderFactory.create_tree_plan_builder(eval_mechanism_params.tree_plan_params)
        pattern_to_tree_plan_map = {pattern: tree_plan_builder.build_tree_plan(statistics_collector.get_statistics(), pattern) for pattern in patterns}

        # Before, was that:
        # return TreeBasedEvaluationMechanism(eval_mechanism_params.updates_time_window, pattern_to_tree_plan_map, eval_mechanism_params.storage_params,
        #                                     statistics_collector,
        #                                     optimizer,
        #                                     tree_manager,
        #                                     eval_mechanism_params.multi_pattern_eval_params)

        if eval_mechanism_params.evaluation_type == TreeEvaluationMechanismTypes.TRIVIAL_TREE_EVALUATION:
            return TrivialEvaluation(pattern_to_tree_plan_map,
                                     eval_mechanism_params.storage_params,
                                     statistics_collector,
                                     optimizer,
                                     eval_mechanism_params.statistics_updates_time_window,
                                     eval_mechanism_params.multi_pattern_eval_params)
        if eval_mechanism_params.evaluation_type == TreeEvaluationMechanismTypes.SIMULTANEOUS_TREE_EVALUATION:
            return SimultaneousEvaluation(pattern_to_tree_plan_map,
                                          eval_mechanism_params.storage_params,
                                          statistics_collector,
                                          optimizer,
                                          eval_mechanism_params.statistics_updates_time_window,
                                          eval_mechanism_params.multi_pattern_eval_params)

    @staticmethod
    def __create_default_eval_parameters():
        """
        Uses the default configuration to create evaluation mechanism parameters.
        """
        if DefaultConfig.DEFAULT_EVALUATION_MECHANISM_TYPE == EvaluationMechanismTypes.TREE_BASED:
            return TreeBasedEvaluationMechanismParameters()
        raise Exception("Unknown evaluation mechanism type: %s" % (DefaultConfig.DEFAULT_EVALUATION_MECHANISM_TYPE,))
