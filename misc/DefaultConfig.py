"""
This file contains the default parameter values for various system configuration settings.
Each of the values below can be overridden by providing a different value in CEP.__init__ or CEP.run.
"""
from datetime import timedelta
from evaluation.EvaluationMechanismTypes import EvaluationMechanismTypes
from misc.SelectionStrategies import SelectionStrategies
from adaptive.statistics.StatisticsTypes import StatisticsTypes
from adaptive.optimizer.OptimizerTypes import OptimizerTypes
from tree.evaluation.TreeEvaluationMechanismUpdateTypes import TreeEvaluationMechanismUpdateTypes
from parallel.ParallelExecutionModes import ParallelExecutionModes
from parallel.ParallelExecutionPlatforms import ParallelExecutionPlatforms
from plan.IterativeImprovement import IterativeImprovementType, IterativeImprovementInitType
from plan.multi.MultiPatternEvaluationApproaches import MultiPatternEvaluationApproaches
from plan.TreeCostModels import TreeCostModels
from plan.TreePlanBuilderTypes import TreePlanBuilderTypes


# general settings
DEFAULT_EVALUATION_MECHANISM_TYPE = EvaluationMechanismTypes.TREE_BASED

# plan generation-related defaults
DEFAULT_TREE_PLAN_BUILDER = TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE
DEFAULT_TREE_COST_MODEL = TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL

# default selection strategies
PRIMARY_SELECTION_STRATEGY = SelectionStrategies.MATCH_ANY
SECONDARY_SELECTION_STRATEGY = SelectionStrategies.MATCH_SINGLE

# tree storage settings
SHOULD_SORT_STORAGE = False
CLEANUP_INTERVAL = 10  # the default number of pattern match additions between subsequent storage cleanups
PRIORITIZE_SORTING_BY_TIMESTAMP = True

# iterative improvement defaults
ITERATIVE_IMPROVEMENT_TYPE = IterativeImprovementType.SWAP_BASED
ITERATIVE_IMPROVEMENT_INIT_TYPE = IterativeImprovementInitType.RANDOM

# multi-pattern optimization defaults
MULTI_PATTERN_APPROACH = MultiPatternEvaluationApproaches.TRIVIAL_SHARING_LEAVES

# parallel execution settings
DEFAULT_PARALLEL_EXECUTION_MODE = ParallelExecutionModes.SEQUENTIAL
DEFAULT_PARALLEL_EXECUTION_PLATFORM = ParallelExecutionPlatforms.THREADING

# optimizer settings
DEFAULT_OPTIMIZER_TYPE = OptimizerTypes.STATISTICS_DEVIATION_AWARE_OPTIMIZER
DEFAULT_INIT_TREE_PLAN_BUILDER = TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE  # initial tree plan builder in case of predifined statistics
DEVIATION_OPTIMIZER_THRESHOLD = 0.5  # the default threshold for statistics changes aware optimizer
DEFAULT_TREE_UPDATE_TYPE = TreeEvaluationMechanismUpdateTypes.SIMULTANEOUS_TREE_EVALUATION
DEFAULT_STATISTICS_TYPE = [StatisticsTypes.ARRIVAL_RATES, StatisticsTypes.SELECTIVITY_MATRIX]  # the default statistics type can also be a list of types
STATISTICS_TIME_WINDOW = timedelta(hours=1)  # Time window for statistics
STATISTICS_UPDATES_WAIT_TIME = None  # the default wait time between statistics updates or None to disable adaptivity
