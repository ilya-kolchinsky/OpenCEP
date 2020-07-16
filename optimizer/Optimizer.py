from enum import Enum
from optimizer.ReoptimizingDecision import ReoptimizationDecisionTypes, UnconditionalPeriodicalAdaptationDecision \
    ,StaticThresholdBasedDecision, InvariantBasedDecision
from optimizer.PlanGenerationAlgorithm import SortingAlgorithm
from evaluation.EvaluationMechanismFactory import EvaluationMechanismTypes

class StatisticsCollectorDataTypes(Enum):
    """
    The various types of data received from the statistics collector.
    """
    EVENTS_RATE = 0

"""
    I NEED SC_data WILL BE A COPY OF WHAT STAS WILL HAVE IN THE STATISTICS COLLECTOR
"""

class Optimizer:
    def __init__(self, SC_data_type : StatisticsCollectorDataTypes, reoptimizing_decision_params: ReoptimizationDecisionTypes,
                 eval_mechanism_type: EvaluationMechanismTypes):
        """
        SC_data - data from the statistics collector
        data_type - type of data received from the statistics collector
        """
        self.data_type = SC_data_type
        self.reoptimizing_decision_type = reoptimizing_decision_params.type
        # self.extract_data_by_type(SC_data, SC_data_type)
        self.reoptimizing_decision = self.__create_reoptimization_decision(reoptimizing_decision_params, SC_data_type)
        self.plan_generation_algorithm = self.__create_plan_generation_algorithm(SC_data_type)
        self.eval_mechanism_type_builder = self.__create_plan_builder_by_type(eval_mechanism_type) # MAYBE NOT NEEDED

    @staticmethod
    def __extract_data_by_type(SC_data: StatisticsCollectorData, data_type : StatisticsCollectorDataTypes):
        """
        Extracting the data received from the statistics collector based on the data type
        """
        if(data_type == StatisticsCollectorDataTypes.EVENTS_RATE):
            """
            Handle data and extract it to variables. Should receive an array of rates, a matrix of selectivities...
            """
    @staticmethod
    def __create_reoptimization_decision(reoptimization_decision_params, SC_data_type):
        if reoptimization_decision_params.type == ReoptimizationDecisionTypes.UNCONDITIONAL_PERIODICAL_ADAPTATION:
            return UnconditionalPeriodicalAdaptationDecision(time_limit=reoptimization_decision_params.time_limit)
        elif reoptimization_decision_params.type == ReoptimizationDecisionTypes.STATIC_THRESHOLD_BASED:
            return StaticThresholdBasedDecision(threshold=reoptimization_decision_params.threshold)
        elif reoptimization_decision_params.type == ReoptimizationDecisionTypes.INVARIANT_BASED:
            return InvariantBasedDecision(SC_data_type)
        else:
            raise NotImplementedError()

    @staticmethod
    def __create_plan_generation_algorithm(SC_data_type: StatisticsCollectorDataTypes):
        if SC_data_type == StatisticsCollectorDataTypes.EVENTS_RATE:
            return SortingAlgorithm()

    @staticmethod
    def __create_plan_builder_by_type(SC_data_type: StatisticsCollectorDataTypes):

    def create_new_plan(self, SC_data: StatisticsCollectorData):
        if self.reoptimizing_decision.decision(SC_data, self.data_type):
            new_plan = self.plan_generation_algorithm(SC_data)
            if self.reoptimizing_decision_type == ReoptimizationDecisionTypes.INVARIANT_BASED:
                self.reoptimizing_decision.gen_new_invariants(new_plan)
            return # Still need to be implemented - Building a tree or an NFA depends on the input
        else:
            return None
