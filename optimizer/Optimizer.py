from optimizer.ReoptimizingDecision import ReoptimizationDecisionTypes, UnconditionalPeriodicalAdaptationDecision
from evaluation.EvaluationMechanismFactory import EvaluationMechanismTypes
from statisticsCollector.StatisticsTypes import StatisticsTypes
from optimizer.Invariants import create_invariant_based_on_data_type
from optimizer.ThresholdDecision import create_threshold_based_on_data_type
from base.Pattern import Pattern
from statisticsCollector.StatisticsCollector import Stat
from evaluation.EvaluationMechanismFactory import EvaluationMechanismFactory


class Optimizer:
    def __init__(self, pattern: Pattern,
                 eval_mechanism_type: EvaluationMechanismTypes,
                 statistics_type: StatisticsTypes,
                 reoptimizing_decision_params,
                 eval_mechanism_params=None
                 ):
        """
        Constructor of class
        """
        self.data_type = statistics_type
        self.reoptimizing_decision_type = reoptimizing_decision_params.type
        self.reoptimizing_decision = self.__create_reoptimization_decision(reoptimizing_decision_params, statistics_type)
        self.pattern = pattern
        self.evaluation_mechanism_type = eval_mechanism_type
        self.eval_mechanism_params = eval_mechanism_params

    @staticmethod
    def __create_reoptimization_decision(reoptimizing_decision_params, statistics_type):
        if reoptimizing_decision_params.type == ReoptimizationDecisionTypes.UNCONDITIONAL_PERIODICAL_ADAPTATION:
            return UnconditionalPeriodicalAdaptationDecision(time_limit=reoptimizing_decision_params.data)
        elif reoptimizing_decision_params.type == ReoptimizationDecisionTypes.STATIC_THRESHOLD_BASED:
            return create_threshold_based_on_data_type(statistics_type, threshold=reoptimizing_decision_params.data)
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
                new_order_for_plan, new_invariants = EvaluationMechanismFactory.build_adaptive_single_pattern_eval_mechanism(
                                                                                        self.evaluation_mechanism_type,
                                                                                        self.eval_mechanism_params,
                                                                                        self.pattern, stat,
                                                                                        is_using_invariants)
                self.reoptimizing_decision.set_new_invariants(new_invariants)
            else:
                new_order_for_plan = EvaluationMechanismFactory.build_adaptive_single_pattern_eval_mechanism(
                                                                                        self.evaluation_mechanism_type,
                                                                                        self.eval_mechanism_params,
                                                                                        self.pattern, stat,
                                                                                        is_using_invariants)
            return new_order_for_plan
        else:
            return None




    def testing_invariants(self, stat: Stat):
        """
        A function for testing
        """
        is_using_invariants = False
        if self.reoptimizing_decision_type == ReoptimizationDecisionTypes.INVARIANT_BASED:
            is_using_invariants = True
        if self.reoptimizing_decision.decision(stat):
            if is_using_invariants:
                new_plan, new_invariants = EvaluationMechanismFactory.build_adaptive_single_pattern_eval_mechanism(
                                                                                        self.evaluation_mechanism_type,
                                                                                        self.eval_mechanism_params,
                                                                                        self.pattern, stat,
                                                                                        is_using_invariants)
                self.reoptimizing_decision.set_new_invariants(new_invariants)
            else:
                new_plan = EvaluationMechanismFactory.build_adaptive_single_pattern_eval_mechanism(
                                                                                        self.evaluation_mechanism_type,
                                                                                        self.eval_mechanism_params,
                                                                                        self.pattern, stat,
                                                                                        is_using_invariants)
            if is_using_invariants:
                return new_plan, new_invariants
            else:
                return new_plan
        else:
            return None, None
