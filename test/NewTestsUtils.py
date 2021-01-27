from datetime import timedelta
from evaluation.EvaluationMechanismFactory import TreeBasedEvaluationMechanismParameters
from misc.OptimizerTypes import OptimizerTypes
from misc.Tree_Evaluation_Mechanism_Types import TreeEvaluationMechanismTypes
from optimizer.OptimizerFactory import OptimizerParameters, StatisticChangesAwareOptimizerParameters
from plan.TreePlanBuilderFactory import TreePlanBuilderParameters, TreeCostModels, StatisticsTypes
from plan.TreePlanBuilderTypes import TreePlanBuilderTypes
from statistics_collector.NewStatCollectorFactory import StatCollectorParameters
from statistics_collector.NewStatisticsFactory import StatisticsParameters
from tree.PatternMatchStorage import TreeStorageParameters

"""
Default testing tree builder settings
"""
DEFAULT_TESTING_STATISTICS_COLLECTOR_ARRIVAL_RATES_STATISTICS = \
    StatCollectorParameters(StatisticsParameters(stat_type=StatisticsTypes.ARRIVAL_RATES))

DEFAULT_TESTING_STATISTICS_COLLECTOR_SELECTIVITY_STATISTICS = \
    StatCollectorParameters(StatisticsParameters(stat_type=StatisticsTypes.SELECTIVITY_MATRIX))

DEFAULT_TESTING_STATISTICS_COLLECTOR_SELECTIVITY_AND_ARRIVAL_RATES_STATISTICS = \
    StatCollectorParameters(StatisticsParameters(stat_type=StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES))

"""
Default testing tree builder settings
"""
DEFAULT_TESTING_INVARIANT_AWARE_GREEDY_TREE_BUILDER = \
    TreePlanBuilderParameters(TreePlanBuilderTypes.INVARIANT_AWARE_GREEDY_LEFT_DEEP_TREE,
                              TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL)

DEFAULT_TESTING_INVARIANT_AWARE_ZSTREAM_BUSHY_TREE_BUILDER = \
    TreePlanBuilderParameters(TreePlanBuilderTypes.INVARIANT_AWARE_ZSTREAM_BUSHY_TREE,
                              TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL)

DEFAULT_TESTING_ZSTREAM_BUSHY_TREE_BUILDER = \
    TreePlanBuilderParameters(TreePlanBuilderTypes.ZSTREAM_BUSHY_TREE,
                              TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL)
"""
Default testing optimizer settings
"""
DEFAULT_TESTING_TRIVIAL_OPTIMIZER_SETTINGS = \
    OptimizerParameters(OptimizerTypes.TRIVIAL, TreePlanBuilderParameters())

DEFAULT_TESTING_CHANGED_BY_T_OPTIMIZER_SETTINGS = \
    StatisticChangesAwareOptimizerParameters(OptimizerTypes.CHANGED_BY_T, TreePlanBuilderParameters(), t=0.5,
                                             stat_type=StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES)

DEFAULT_TESTING_GREEDY_INVARIANT_OPTIMIZER_SETTINGS = \
    OptimizerParameters(OptimizerTypes.USING_INVARIANT, DEFAULT_TESTING_INVARIANT_AWARE_GREEDY_TREE_BUILDER)

DEFAULT_TESTING_ZSTREAM_INVARIANT_OPTIMIZER_SETTINGS = \
    OptimizerParameters(OptimizerTypes.USING_INVARIANT, DEFAULT_TESTING_INVARIANT_AWARE_ZSTREAM_BUSHY_TREE_BUILDER)

DEFAULT_TESTING_TRIVIAL_OPTIMIZER_SETTINGS_WITH_ZSTREAM = \
    OptimizerParameters(OptimizerTypes.TRIVIAL, DEFAULT_TESTING_ZSTREAM_BUSHY_TREE_BUILDER)


"""
Default testing Evaluation mechanism settings
"""
# trivial evaluation mechanism
# trivial optimizer
# trivial left tree builder
DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS = \
    TreeBasedEvaluationMechanismParameters(timedelta(seconds=0.001),
                                           TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                                                     TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL),
                                           TreeStorageParameters(sort_storage=False,
                                                                 clean_up_interval=10,
                                                                 prioritize_sorting_by_timestamp=True),
                                           evaluation_type=TreeEvaluationMechanismTypes.TRIVIAL_TREE_EVALUATION)

# trivial evaluation mechanism
# t optimizer
# trivial left tree builder
DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER = \
    TreeBasedEvaluationMechanismParameters(timedelta(seconds=0.01),
                                           TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                                                     TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL),
                                           TreeStorageParameters(sort_storage=False,
                                                                 clean_up_interval=10,
                                                                 prioritize_sorting_by_timestamp=True),
                                           evaluation_type=TreeEvaluationMechanismTypes.TRIVIAL_TREE_EVALUATION,
                                           optimizer_params=DEFAULT_TESTING_CHANGED_BY_T_OPTIMIZER_SETTINGS,
                                           statistics_collector_params=StatCollectorParameters(StatisticsParameters(time_window=timedelta(seconds=2), stat_type=StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES)))

# trivial evaluation mechanism
# greedy invariant optimizer
# trivial left tree builder
# selectivity and arrival rates statistics collector
DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER = \
    TreeBasedEvaluationMechanismParameters(timedelta(seconds=0.001),
                                           TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                                                     TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL),
                                           TreeStorageParameters(sort_storage=False,
                                                                 clean_up_interval=10,
                                                                 prioritize_sorting_by_timestamp=True),
                                           evaluation_type=TreeEvaluationMechanismTypes.TRIVIAL_TREE_EVALUATION,
                                           statistics_collector_params=DEFAULT_TESTING_STATISTICS_COLLECTOR_SELECTIVITY_AND_ARRIVAL_RATES_STATISTICS,
                                           optimizer_params=DEFAULT_TESTING_GREEDY_INVARIANT_OPTIMIZER_SETTINGS)


# trivial evaluation mechanism
# zstream invariant optimizer
# zstream tree builder
# selectivity and arrival rates statistics collector
DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER = \
    TreeBasedEvaluationMechanismParameters(timedelta(seconds=0.05),
                                           TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                                                     TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL),
                                           TreeStorageParameters(sort_storage=False,
                                                                 clean_up_interval=10,
                                                                 prioritize_sorting_by_timestamp=True),
                                           evaluation_type=TreeEvaluationMechanismTypes.TRIVIAL_TREE_EVALUATION,
                                           statistics_collector_params=DEFAULT_TESTING_STATISTICS_COLLECTOR_SELECTIVITY_AND_ARRIVAL_RATES_STATISTICS,
                                           optimizer_params=DEFAULT_TESTING_ZSTREAM_INVARIANT_OPTIMIZER_SETTINGS)


# simultaneous evaluation mechanism
# trivial optimizer
# trivial left tree builder
DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS = \
    TreeBasedEvaluationMechanismParameters(timedelta(seconds=0.05),
                                           TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                                                     TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL),
                                           TreeStorageParameters(sort_storage=False,
                                                                 clean_up_interval=10,
                                                                 prioritize_sorting_by_timestamp=True),
                                           evaluation_type=TreeEvaluationMechanismTypes.SIMULTANEOUS_TREE_EVALUATION)


# simultaneous evaluation mechanism
# t optimizer
# trivial left tree builder
DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER = \
    TreeBasedEvaluationMechanismParameters(timedelta(seconds=0.05),
                                           TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                                                     TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL),
                                           TreeStorageParameters(sort_storage=False,
                                                                 clean_up_interval=10,
                                                                 prioritize_sorting_by_timestamp=True),
                                           evaluation_type=TreeEvaluationMechanismTypes.SIMULTANEOUS_TREE_EVALUATION,
                                           optimizer_params=DEFAULT_TESTING_CHANGED_BY_T_OPTIMIZER_SETTINGS,
                                           statistics_collector_params=StatCollectorParameters(StatisticsParameters(stat_type=StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES)))


# simultaneous evaluation mechanism
# greedy invariant optimizer
# greedy tree builder
# selectivity and arrival rates statistics collector
DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER = \
    TreeBasedEvaluationMechanismParameters(timedelta(seconds=0.001),
                                           TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                                                     TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL),
                                           TreeStorageParameters(sort_storage=False,
                                                                 clean_up_interval=10,
                                                                 prioritize_sorting_by_timestamp=True),
                                           evaluation_type=TreeEvaluationMechanismTypes.SIMULTANEOUS_TREE_EVALUATION,
                                           statistics_collector_params=DEFAULT_TESTING_STATISTICS_COLLECTOR_SELECTIVITY_AND_ARRIVAL_RATES_STATISTICS,
                                           optimizer_params=DEFAULT_TESTING_GREEDY_INVARIANT_OPTIMIZER_SETTINGS)


# simultaneous evaluation mechanism
# greedy invariant optimizer
# greedy tree builder
# selectivity and arrival rates statistics collector
DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER = \
    TreeBasedEvaluationMechanismParameters(timedelta(seconds=0.01),
                                           TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                                                     TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL),
                                           TreeStorageParameters(sort_storage=False,
                                                                 clean_up_interval=10,
                                                                 prioritize_sorting_by_timestamp=True),
                                           evaluation_type=TreeEvaluationMechanismTypes.SIMULTANEOUS_TREE_EVALUATION,
                                           statistics_collector_params=DEFAULT_TESTING_STATISTICS_COLLECTOR_SELECTIVITY_AND_ARRIVAL_RATES_STATISTICS,
                                           optimizer_params=DEFAULT_TESTING_ZSTREAM_INVARIANT_OPTIMIZER_SETTINGS)

