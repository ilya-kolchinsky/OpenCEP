from optimizer.ReoptimizingDecision import ReoptimizationDecisionTypes
from statisticsCollector.StatisticsTypes import StatisticsTypes
from evaluation.TreeBasedEvaluationMechanism import TreeReplacementAlgorithmTypes
from datetime import timedelta
from statisticsCollector.DataManagerAlgorithm import DataManagerAlgorithmTypes


class ReoptimizingDecisionParameters():
    """
    A struct that includes the repotimization_decision_type and it's data accordingly. Will be used to initialize the
    Optimizer.
    For ReoptimizationDecisionTypes.UNCONDITIONAL_PERIODICAL_ADAPTATION data will be the time_limit in seconds
    For ReoptimizationDecisionTypes.STATIC_THRESHOLD_BASED data will be the threshold
    For ReoptimizationDecisionTypes.INVARIANT_BASED data will be None
    In order to use ReoptimizationDecisionTypes.INVARIANT_BASED, the evaluation mechanism type (which is received in
    the CEP constructor) will have to be EvaluationMechanismTypes.GREEDY_LEFT_DEEP_TREE
    """
    def __init__(self, type: ReoptimizationDecisionTypes):
        self.type = type


class UnconditionalPeriodicalParameters(ReoptimizingDecisionParameters):
    def __init__(self, type: ReoptimizationDecisionTypes, time_limit_data: float):
        super().__init__(type)
        self.data = time_limit_data


class RelativeThresholdlParameters(ReoptimizingDecisionParameters):
    def __init__(self, type: ReoptimizationDecisionTypes, threshold_data: float):
        super().__init__(type)
        self.data = threshold_data


class InvariantsParameters(ReoptimizingDecisionParameters):
    def __init__(self, type: ReoptimizationDecisionTypes, invariant_data):
        super().__init__(type)
        self.data = None


class AdaptiveParameters:
    """
    This class is meant for delivering the parameters for an adaptive CEP run.
    The values of the parameters should be inserted by the user according to his needs.
    Every EvaluationMechanismBuilder is using a certain statistics_type and needs to get it in order to work.
    Here is the list of the evaluation_mechanism_types and the optional statistics_types accordingly:
    EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE : []
    EvaluationMechanismTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE : [StatisticsTypes.FREQUENCY_DICT, StatisticsTypes.ARRIVAL_RATES]
    EvaluationMechanismTypes.GREEDY_LEFT_DEEP_TREE : [StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES]
    EvaluationMechanismTypes.LOCAL_SEARCH_LEFT_DEEP_TREE : [StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES]
    EvaluationMechanismTypes.DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE : [StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES]
    EvaluationMechanismTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE : [StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES]
    EvaluationMechanismTypes.ZSTREAM_BUSHY_TREE : [StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES]
    EvaluationMechanismTypes.ORDERED_ZSTREAM_BUSHY_TREE : [StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES]
    """
    def __init__(self, statistics_type: StatisticsTypes, reoptimizing_decision_params: ReoptimizingDecisionParameters,
                 tree_replacement_algorithm_type=TreeReplacementAlgorithmTypes.IMMEDIATE_REPLACE_TREE,
                 activate_statistics_collector_period: timedelta = timedelta(minutes=10),
                 activate_optimizer_period: timedelta = timedelta(minutes=10),
                 data_manager_type: DataManagerAlgorithmTypes = DataManagerAlgorithmTypes.TRIVIAL_ALGORITHM):
        """
        Statistics Collector parameters
        """
        self.statistics_type = statistics_type  # See class StatisticsTypes(Enum)
        self.data_manager_type = data_manager_type
        self.activate_statistics_collector_period = activate_statistics_collector_period  # This variable will decide how often (in time_delta) StatisticsCollector will be supply statistics
        """
        Optimizer parameters
        """
        self.activate_optimizer_period = activate_optimizer_period  # This variable will decide how often (in time_delta) Optimizer will be activated
        self.reoptimizing_decision_params = reoptimizing_decision_params
        """
        Evaluation Mechanism parameters
        """
        self.tree_replacement_algorithm_type = tree_replacement_algorithm_type  # See class ManageTreeType(Enum)
