from datetime import timedelta
from evaluation.EvaluationMechanismFactory import TreeBasedEvaluationMechanismParameters
from misc.OptimizerTypes import OptimizerTypes
from misc.TreeEvaluationMechanismTypes import TreeEvaluationMechanismTypes
from optimizer.OptimizerFactory import OptimizerParameters, StatisticChangesAwareOptimizerParameters, \
    InvariantsAwareOptimizerParameters, TrivialOptimizerParameters
from plan.TreePlanBuilderFactory import TreePlanBuilderParameters, TreeCostModels, StatisticsTypes
from plan.TreePlanBuilderTypes import TreePlanBuilderTypes
from statistics_collector.StatisticsCollectorFactory import StatisticsCollectorParameters
from tree.PatternMatchStorage import TreeStorageParameters

"""
Default testing statistics collector settings
"""
DEFAULT_TESTING_STATISTICS_COLLECTOR_ARRIVAL_RATES_STATISTICS = \
    StatisticsCollectorParameters(statistics_types=StatisticsTypes.ARRIVAL_RATES)

DEFAULT_TESTING_STATISTICS_COLLECTOR_SELECTIVITY_STATISTICS = \
    StatisticsCollectorParameters(statistics_types=StatisticsTypes.SELECTIVITY_MATRIX)

DEFAULT_TESTING_STATISTICS_COLLECTOR_SELECTIVITY_AND_ARRIVAL_RATES_STATISTICS = \
    StatisticsCollectorParameters(statistics_types=[StatisticsTypes.SELECTIVITY_MATRIX, StatisticsTypes.ARRIVAL_RATES])

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
    TrivialOptimizerParameters(TreePlanBuilderParameters())

DEFAULT_TESTING_CHANGES_AWARE_OPTIMIZER_SETTINGS = \
    StatisticChangesAwareOptimizerParameters(TreePlanBuilderParameters(), t=0.5,
                                             statistics_types=StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES)

DEFAULT_TESTING_GREEDY_INVARIANT_OPTIMIZER_SETTINGS = \
    InvariantsAwareOptimizerParameters(DEFAULT_TESTING_INVARIANT_AWARE_GREEDY_TREE_BUILDER)

DEFAULT_TESTING_ZSTREAM_INVARIANT_OPTIMIZER_SETTINGS = \
    InvariantsAwareOptimizerParameters(DEFAULT_TESTING_INVARIANT_AWARE_ZSTREAM_BUSHY_TREE_BUILDER)

DEFAULT_TESTING_TRIVIAL_OPTIMIZER_SETTINGS_WITH_ZSTREAM = \
    TrivialOptimizerParameters(DEFAULT_TESTING_ZSTREAM_BUSHY_TREE_BUILDER)

"""
Default testing Evaluation mechanism settings
"""

"""
statistics collector: arrival rate
evaluation mechanism: trivial
optimizer: trivial
tree builder: trivial left tree builder
"""
DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS = \
    TreeBasedEvaluationMechanismParameters(TreeStorageParameters(sort_storage=False,
                                                                 clean_up_interval=10,
                                                                 prioritize_sorting_by_timestamp=True),
                                           evaluation_type=TreeEvaluationMechanismTypes.TRIVIAL_TREE_EVALUATION,
                                           statistics_updates_time_window=timedelta(seconds=0.001))

"""
statistics collector: selectivity and arrival rates
evaluation mechanism: trivial
optimizer: changes aware optimizer
tree builder: trivial left tree builder
"""
DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER = \
    TreeBasedEvaluationMechanismParameters(TreeStorageParameters(sort_storage=False,
                                                                 clean_up_interval=10,
                                                                 prioritize_sorting_by_timestamp=True),
                                           evaluation_type=TreeEvaluationMechanismTypes.TRIVIAL_TREE_EVALUATION,
                                           optimizer_params=DEFAULT_TESTING_CHANGES_AWARE_OPTIMIZER_SETTINGS,
                                           statistics_collector_params=StatisticsCollectorParameters(
                                                                    time_window=timedelta(seconds=2),
                                                                    statistics_types=[StatisticsTypes.SELECTIVITY_MATRIX, StatisticsTypes.ARRIVAL_RATES]),
                                           statistics_updates_time_window=timedelta(seconds=0.01))


"""
statistics collector: selectivity and arrival rates
evaluation mechanism: trivial
optimizer: greedy invariant
tree builder: trivial left tree builder
"""
DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER = \
    TreeBasedEvaluationMechanismParameters(TreeStorageParameters(sort_storage=False,
                                                                 clean_up_interval=10,
                                                                 prioritize_sorting_by_timestamp=True),
                                           evaluation_type=TreeEvaluationMechanismTypes.TRIVIAL_TREE_EVALUATION,
                                           statistics_collector_params=DEFAULT_TESTING_STATISTICS_COLLECTOR_SELECTIVITY_AND_ARRIVAL_RATES_STATISTICS,
                                           optimizer_params=DEFAULT_TESTING_GREEDY_INVARIANT_OPTIMIZER_SETTINGS,
                                           statistics_updates_time_window=timedelta(seconds=0.001))

"""
statistics collector: selectivity and arrival rates
evaluation mechanism: trivial
optimizer: zstream invariant
tree builder: trivial left tree builder
"""
DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER = \
    TreeBasedEvaluationMechanismParameters(TreeStorageParameters(sort_storage=False,
                                                                 clean_up_interval=10,
                                                                 prioritize_sorting_by_timestamp=True),
                                           evaluation_type=TreeEvaluationMechanismTypes.TRIVIAL_TREE_EVALUATION,
                                           statistics_collector_params=DEFAULT_TESTING_STATISTICS_COLLECTOR_SELECTIVITY_AND_ARRIVAL_RATES_STATISTICS,
                                           optimizer_params=DEFAULT_TESTING_ZSTREAM_INVARIANT_OPTIMIZER_SETTINGS,
                                           statistics_updates_time_window=timedelta(seconds=0.001))

"""
statistics collector: arrival rates
evaluation mechanism: simultaneous
optimizer: trivial
tree builder: trivial left tree builder
"""
DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS = \
    TreeBasedEvaluationMechanismParameters(TreeStorageParameters(sort_storage=False,
                                                                 clean_up_interval=10,
                                                                 prioritize_sorting_by_timestamp=True),
                                           evaluation_type=TreeEvaluationMechanismTypes.SIMULTANEOUS_TREE_EVALUATION,
                                           statistics_updates_time_window=timedelta(seconds=0.05))


"""
statistics collector: selectivity and arrival rates
evaluation mechanism: simultaneous
optimizer: changes aware
tree builder: trivial left tree builder
"""
DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER = \
    TreeBasedEvaluationMechanismParameters(TreeStorageParameters(sort_storage=False,
                                                                 clean_up_interval=10,
                                                                 prioritize_sorting_by_timestamp=True),
                                           evaluation_type=TreeEvaluationMechanismTypes.SIMULTANEOUS_TREE_EVALUATION,
                                           optimizer_params=DEFAULT_TESTING_CHANGES_AWARE_OPTIMIZER_SETTINGS,
                                           statistics_collector_params=StatisticsCollectorParameters(
                                            statistics_types=[StatisticsTypes.SELECTIVITY_MATRIX, StatisticsTypes.ARRIVAL_RATES]),
                                           statistics_updates_time_window=timedelta(seconds=0.05))


"""
statistics collector: selectivity and arrival rates
evaluation mechanism: simultaneous
optimizer: greedy invariant
tree builder: trivial left tree builder
"""
DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER = \
    TreeBasedEvaluationMechanismParameters(TreeStorageParameters(sort_storage=False,
                                                                 clean_up_interval=10,
                                                                 prioritize_sorting_by_timestamp=True),
                                           evaluation_type=TreeEvaluationMechanismTypes.SIMULTANEOUS_TREE_EVALUATION,
                                           statistics_collector_params=DEFAULT_TESTING_STATISTICS_COLLECTOR_SELECTIVITY_AND_ARRIVAL_RATES_STATISTICS,
                                           optimizer_params=DEFAULT_TESTING_GREEDY_INVARIANT_OPTIMIZER_SETTINGS,
                                           statistics_updates_time_window=timedelta(seconds=0.001))


"""
statistics collector: selectivity and arrival rates
evaluation mechanism: simultaneous
optimizer: greedy invariant
tree builder: greedy tree builder
"""
DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER = \
    TreeBasedEvaluationMechanismParameters(TreeStorageParameters(sort_storage=False,
                                                                 clean_up_interval=10,
                                                                 prioritize_sorting_by_timestamp=True),
                                           evaluation_type=TreeEvaluationMechanismTypes.SIMULTANEOUS_TREE_EVALUATION,
                                           statistics_collector_params=DEFAULT_TESTING_STATISTICS_COLLECTOR_SELECTIVITY_AND_ARRIVAL_RATES_STATISTICS,
                                           optimizer_params=DEFAULT_TESTING_ZSTREAM_INVARIANT_OPTIMIZER_SETTINGS,
                                           statistics_updates_time_window=timedelta(seconds=0.01))
