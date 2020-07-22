from enum import Enum
from optimizer.ReoptimizingDecision import ReoptimizationDecisionTypes, UnconditionalPeriodicalAdaptationDecision \
    ,StaticThresholdBasedDecision, InvariantBasedDecision
from evaluation.EvaluationMechanismFactory import EvaluationMechanismTypes
from misc.StatisticsTypes import StatisticsTypes
from optimizer.Invariants import create_invariant_based_on_data_type
from optimizer.ThresholdDecision import create_threshold_based_on_data_type
from optimizer.PlanGenerationAlgorithm import GreedyAlgorithm, PlanBuilder
from base.Pattern import Pattern


class Optimizer:
    def __init__(self, SC_data_type: StatisticsTypes,
                 reoptimizing_decision_params,
                 eval_mechanism_type: EvaluationMechanismTypes,
                 pattern: Pattern):
        """
        reoptimizing_decision_params will include the type of the reoptimizing decision (ReoptimizationDecisionTypes)
        and another parameters according to the type (for example if the type is UNCONDITIONAL_PERIODICAL_ADAPTATION,
        reoptimizing_decision_params will include a variable time_limit which will have the time limit before changing
        the current plan
        """
        self.data_type = SC_data_type
        self.reoptimizing_decision_type = reoptimizing_decision_params.type
        # self.extract_data_by_type(SC_data, SC_data_type)
        self.reoptimizing_decision = self.__create_reoptimization_decision(reoptimizing_decision_params, SC_data_type)
        self.plan_generation_algorithm = self.__create_plan_generation_algorithm(SC_data_type, reoptimizing_decision_params.type)
        #self.eval_mechanism_type_builder = self.__create_plan_builder_by_type(eval_mechanism_type) # MAYBE NOT NEEDED
        self.pattern = pattern

    @staticmethod
    def __extract_data_by_type(SC_data, data_type: StatisticsTypes):
        """
        Extracting the data received from the statistics collector based on the data type
        """
        if(data_type == StatisticsTypes.ARRIVAL_RATES):
            """
            Handle data and extract it to variables. Should receive an array of rates, a matrix of selectivities...
            """
    @staticmethod
    def __create_reoptimization_decision(reoptimization_decision_params, SC_data_type):
        if reoptimization_decision_params.type == ReoptimizationDecisionTypes.UNCONDITIONAL_PERIODICAL_ADAPTATION:
            return UnconditionalPeriodicalAdaptationDecision(time_limit=reoptimization_decision_params.time_limit)
        elif reoptimization_decision_params.type == ReoptimizationDecisionTypes.STATIC_THRESHOLD_BASED:
            return create_threshold_based_on_data_type(SC_data_type, threshold=reoptimization_decision_params.threshold)
        elif reoptimization_decision_params.type == ReoptimizationDecisionTypes.INVARIANT_BASED:
            return create_invariant_based_on_data_type(SC_data_type)
        else:
            raise NotImplementedError()

    @staticmethod
    def __create_plan_generation_algorithm(SC_data_type, reoptimizing_decision_type):
        if SC_data_type == StatisticsTypes.ARRIVAL_RATES and reoptimizing_decision_type == ReoptimizationDecisionTypes.INVARIANT_BASED:
            return GreedyAlgorithm()
        else:
            return PlanBuilder()

    #@staticmethod
    #def __create_plan_builder_by_type(SC_data_type: StatisticsTypes):

    def run(self, SC_data):
        if self.reoptimizing_decision.decision(SC_data):
            new_plan = self.plan_generation_algorithm.generate_plan(SC_data)
            if self.reoptimizing_decision_type == ReoptimizationDecisionTypes.INVARIANT_BASED:
                self.reoptimizing_decision.gen_new_invariants(new_plan)
                return self.plan_generation_algorithm.build_final_plan_for_evaluation(self.pattern, new_plan)
            else:
                return self.plan_generation_algorithm.build_final_plan_for_evaluation(self.data_type, self.pattern)
        else:
            return None

    def testing_decisions(self, SC_data):
        return self.reoptimizing_decision.decision(SC_data)
