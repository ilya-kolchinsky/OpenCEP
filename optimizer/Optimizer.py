from optimizer.ReoptimizingDecision import ReoptimizationDecisionTypes, UnconditionalPeriodicalAdaptationDecision, \
                                           RelativeThresholdBasedDecision
from statisticsCollector.StatisticsTypes import StatisticsTypes
from optimizer.Invariants import create_invariant_based_on_data_type
from base.Pattern import Pattern
from statisticsCollector.Stat import Stat
from evaluation.EvaluationMechanismFactory import EvaluationMechanismFactory


class Optimizer:
    def __init__(self, pattern: Pattern, eval_mechanism_type, eval_mechanism_params=None):
        self.data_type = eval_mechanism_params.adaptive_parameters.statistics_type
        self.reoptimizing_decision_type = eval_mechanism_params.adaptive_parameters.reoptimizing_decision_params.type
        self.reoptimizing_decision = self.__create_reoptimization_decision(
                                    eval_mechanism_params.adaptive_parameters.reoptimizing_decision_params,
                                    eval_mechanism_params.adaptive_parameters.statistics_type)
        self.pattern = pattern
        self.evaluation_mechanism_type = eval_mechanism_type
        self.eval_mechanism_params = eval_mechanism_params

    @staticmethod
    def __create_reoptimization_decision(reoptimizing_decision_params, statistics_type: StatisticsTypes):
        """
        Creating reoptimization_decision based on the statistics_type
        """
        if reoptimizing_decision_params.type == ReoptimizationDecisionTypes.UNCONDITIONAL_PERIODICAL_ADAPTATION:
            return UnconditionalPeriodicalAdaptationDecision(time_limit=reoptimizing_decision_params.data)
        elif reoptimizing_decision_params.type == ReoptimizationDecisionTypes.RELATIVE_THRESHOLD_BASED:
            return RelativeThresholdBasedDecision(threshold=reoptimizing_decision_params.data)
        elif reoptimizing_decision_params.type == ReoptimizationDecisionTypes.INVARIANT_BASED:
            return create_invariant_based_on_data_type(statistics_type)
        else:
            raise NotImplementedError()

    def run(self, stat: Stat):
        is_using_invariants = False
        if self.reoptimizing_decision_type == ReoptimizationDecisionTypes.INVARIANT_BASED:
            is_using_invariants = True
        if self.reoptimizing_decision.decision(stat):
            if is_using_invariants:
                temp_tree, new_order_for_plan = EvaluationMechanismFactory.build_single_pattern_eval_mechanism(
                                                                                        self.evaluation_mechanism_type,
                                                                                        self.eval_mechanism_params,
                                                                                        self.pattern, stat, self)
            else:
                temp_tree, new_order_for_plan = EvaluationMechanismFactory.build_single_pattern_eval_mechanism(
                                                                                        self.evaluation_mechanism_type,
                                                                                        self.eval_mechanism_params,
                                                                                        self.pattern, stat)
            return new_order_for_plan
        else:
            return None
