from optimizer.ReoptimizingDecision import ReoptimizationDecisionTypes
from statisticsCollector.StatisticsTypes import StatisticsTypes
from evaluation.AdaptiveTreeReplacementAlgorithm import TreeReplacementAlgorithmTypes

"""

"""
class ReoptimizingDecisionParameters:
    def __init__(self, type, data):
        self.type = type  # See class ReoptimizationDecisionTypes(Enum)
        """
        The data should be according to self.type:
        if self.type == ReoptimizationDecisionTypes.UNCONDITIONAL_PERIODICAL_ADAPTATION should self.data will be the time limit (in seconds)
        if self.type == ReoptimizationDecisionTypes.STATIC_THRESHOLD_BASED then self.data should be the threshold value
        if self.type == ReoptimizationDecisionTypes.INVARIANT_BASED then self.data should be None
        """
        self.data = data


"""
This class is meant for delivering the parameters for an adaptive CEP run.
The values of the parameters should be inserted by the user according to his needs.
"""
class AdaptiveParameters:
    def __init__(self, statistics_type: StatisticsTypes, reoptimizing_decision_params: ReoptimizingDecisionParameters,
                 tree_replacement_algorithm_type=TreeReplacementAlgorithmTypes.STOP_AND_REPLACE_TREE,
                 activate_statistics_collector_period=60, activate_optimizer_period=60,
                 window_coefficient=2, k=3):
        """
        Statistics Collector parameters
        """
        self.statistics_type = statistics_type  # See class StatisticsTypes(Enum)
        self.k = k  # Don't know yet
        self.window_coefficient = window_coefficient
        self.activate_statistics_collector_period = activate_statistics_collector_period  # This variable will decide how often (in seconds) StatisticsCollector will be activated
        """
        Optimizer parameters
        """
        self.activate_optimizer_period = activate_optimizer_period # This variable will decide how often (in seconds) Optimizer will be activated
        self.reoptimizing_decision_params = reoptimizing_decision_params
        """
        Evaluation Mechanism parameters
        """
        # self.evaluation_mechansim_type is also a parameter when CEP is running not adaptively. Should we still insert it here? probably not
        self.tree_replacement_algorithm_type = tree_replacement_algorithm_type  # See class ManageTreeType(Enum)
