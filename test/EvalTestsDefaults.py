from datetime import timedelta
from evaluation.EvaluationMechanismFactory import TreeBasedEvaluationMechanismParameters
from tree.evaluation.TreeEvaluationMechanismUpdateTypes import TreeEvaluationMechanismUpdateTypes
from adaptive.optimizer.OptimizerFactory import StatisticsDeviationAwareOptimizerParameters, \
    InvariantsAwareOptimizerParameters, TrivialOptimizerParameters
from plan.TreePlanBuilderFactory import TreePlanBuilderParameters, TreeCostModels, StatisticsTypes
from plan.TreePlanBuilderTypes import TreePlanBuilderTypes
from adaptive.statistics.StatisticsCollectorFactory import StatisticsCollectorParameters
from tree.PatternMatchStorage import TreeStorageParameters


DEFAULT_TREE_STORAGE_PARAMETERS = TreeStorageParameters(sort_storage=False,
                                                        clean_up_interval=10,
                                                        prioritize_sorting_by_timestamp=True)

"""
Default testing statistics collector settings
"""
DEFAULT_TESTING_STATISTICS_COLLECTOR_SELECTIVITY_AND_ARRIVAL_RATES_STATISTICS = \
    StatisticsCollectorParameters(statistics_types=[StatisticsTypes.SELECTIVITY_MATRIX, StatisticsTypes.ARRIVAL_RATES])

"""
Default testing tree builder settings
"""
DEFAULT_BASIC_TESTING_TREE_BUILDER = \
    TreePlanBuilderParameters(TreePlanBuilderTypes.GREEDY_LEFT_DEEP_TREE,
                              TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL)

DEFAULT_TESTING_INVARIANT_AWARE_GREEDY_TREE_BUILDER = \
    TreePlanBuilderParameters(TreePlanBuilderTypes.INVARIANT_AWARE_GREEDY_LEFT_DEEP_TREE,
                              TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL)

DEFAULT_TESTING_INVARIANT_AWARE_ZSTREAM_BUSHY_TREE_BUILDER = \
    TreePlanBuilderParameters(TreePlanBuilderTypes.INVARIANT_AWARE_ZSTREAM_BUSHY_TREE,
                              TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL)

"""
Default testing optimizer settings
"""
DEFAULT_TESTING_TRIVIAL_OPTIMIZER_SETTINGS = \
    TrivialOptimizerParameters(tree_plan_params=DEFAULT_BASIC_TESTING_TREE_BUILDER,
                               statistics_collector_params=DEFAULT_TESTING_STATISTICS_COLLECTOR_SELECTIVITY_AND_ARRIVAL_RATES_STATISTICS,
                               statistics_updates_wait_time=timedelta(minutes=10))

DEFAULT_TESTING_DEVIATION_AWARE_OPTIMIZER_SETTINGS = \
    StatisticsDeviationAwareOptimizerParameters(tree_plan_params=DEFAULT_BASIC_TESTING_TREE_BUILDER, deviation_threshold=0.5,
                               statistics_collector_params=DEFAULT_TESTING_STATISTICS_COLLECTOR_SELECTIVITY_AND_ARRIVAL_RATES_STATISTICS,
                               statistics_updates_wait_time=timedelta(minutes=10))

DEFAULT_TESTING_GREEDY_INVARIANT_OPTIMIZER_SETTINGS = \
    InvariantsAwareOptimizerParameters(tree_plan_params=DEFAULT_TESTING_INVARIANT_AWARE_GREEDY_TREE_BUILDER,
                               statistics_collector_params=DEFAULT_TESTING_STATISTICS_COLLECTOR_SELECTIVITY_AND_ARRIVAL_RATES_STATISTICS,
                               statistics_updates_wait_time=timedelta(minutes=10))

DEFAULT_TESTING_ZSTREAM_INVARIANT_OPTIMIZER_SETTINGS = \
    InvariantsAwareOptimizerParameters(tree_plan_params=DEFAULT_TESTING_INVARIANT_AWARE_ZSTREAM_BUSHY_TREE_BUILDER,
                               statistics_collector_params=DEFAULT_TESTING_STATISTICS_COLLECTOR_SELECTIVITY_AND_ARRIVAL_RATES_STATISTICS,
                               statistics_updates_wait_time=timedelta(minutes=10))

"""
Default testing Evaluation mechanism settings
"""

"""
evaluation mechanism: trivial
optimizer: trivial
"""
DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS = \
    TreeBasedEvaluationMechanismParameters(storage_params=DEFAULT_TREE_STORAGE_PARAMETERS,
                                           tree_update_type=TreeEvaluationMechanismUpdateTypes.TRIVIAL_TREE_EVALUATION,
                                           optimizer_params=DEFAULT_TESTING_TRIVIAL_OPTIMIZER_SETTINGS)

"""
evaluation mechanism: trivial
optimizer: changes aware optimizer
"""
DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_DEVIATION_AWARE_OPTIMIZER = \
    TreeBasedEvaluationMechanismParameters(storage_params=DEFAULT_TREE_STORAGE_PARAMETERS,
                                           tree_update_type=TreeEvaluationMechanismUpdateTypes.TRIVIAL_TREE_EVALUATION,
                                           optimizer_params=DEFAULT_TESTING_DEVIATION_AWARE_OPTIMIZER_SETTINGS)


"""
evaluation mechanism: trivial
optimizer: greedy invariant
"""
DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER = \
    TreeBasedEvaluationMechanismParameters(storage_params=DEFAULT_TREE_STORAGE_PARAMETERS,
                                           tree_update_type=TreeEvaluationMechanismUpdateTypes.TRIVIAL_TREE_EVALUATION,
                                           optimizer_params=DEFAULT_TESTING_GREEDY_INVARIANT_OPTIMIZER_SETTINGS)

"""
evaluation mechanism: trivial
optimizer: zstream invariant
"""
DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER = \
    TreeBasedEvaluationMechanismParameters(storage_params=DEFAULT_TREE_STORAGE_PARAMETERS,
                                           tree_update_type=TreeEvaluationMechanismUpdateTypes.TRIVIAL_TREE_EVALUATION,
                                           optimizer_params=DEFAULT_TESTING_ZSTREAM_INVARIANT_OPTIMIZER_SETTINGS)

"""
evaluation mechanism: simultaneous
optimizer: trivial
"""
DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS = \
    TreeBasedEvaluationMechanismParameters(storage_params=DEFAULT_TREE_STORAGE_PARAMETERS,
                                           tree_update_type=TreeEvaluationMechanismUpdateTypes.SIMULTANEOUS_TREE_EVALUATION,
                                           optimizer_params=DEFAULT_TESTING_TRIVIAL_OPTIMIZER_SETTINGS)


"""
evaluation mechanism: simultaneous
optimizer: changes aware
"""
DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER = \
    TreeBasedEvaluationMechanismParameters(storage_params=DEFAULT_TREE_STORAGE_PARAMETERS,
                                           tree_update_type=TreeEvaluationMechanismUpdateTypes.SIMULTANEOUS_TREE_EVALUATION,
                                           optimizer_params=DEFAULT_TESTING_DEVIATION_AWARE_OPTIMIZER_SETTINGS)


"""
evaluation mechanism: simultaneous
optimizer: greedy invariant
"""
DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER = \
    TreeBasedEvaluationMechanismParameters(storage_params=DEFAULT_TREE_STORAGE_PARAMETERS,
                                           tree_update_type=TreeEvaluationMechanismUpdateTypes.SIMULTANEOUS_TREE_EVALUATION,
                                           optimizer_params=DEFAULT_TESTING_GREEDY_INVARIANT_OPTIMIZER_SETTINGS)


"""
evaluation mechanism: simultaneous
optimizer: greedy invariant
"""
DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER = \
    TreeBasedEvaluationMechanismParameters(storage_params=DEFAULT_TREE_STORAGE_PARAMETERS,
                                           tree_update_type=TreeEvaluationMechanismUpdateTypes.SIMULTANEOUS_TREE_EVALUATION,
                                           optimizer_params=DEFAULT_TESTING_ZSTREAM_INVARIANT_OPTIMIZER_SETTINGS)
