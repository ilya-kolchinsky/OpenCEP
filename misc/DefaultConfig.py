"""
This file contains the default parameter values for various system configuration settings.
Each of the values below can be overridden by providing a different value in CEP.__init__ or CEP.run.
"""
from datetime import timedelta
from evaluation.EvaluationMechanismTypes import EvaluationMechanismTypes
from misc.SelectionStrategies import SelectionStrategies
from adaptive.statistics.StatisticsTypes import StatisticsTypes
from adaptive.optimizer.OptimizerTypes import OptimizerTypes
from plan.multi.local_search.LocalSearchApproaches import LocalSearchApproaches
from tree.evaluation.TreeEvaluationMechanismUpdateTypes import TreeEvaluationMechanismUpdateTypes
from parallel.ParallelExecutionModes import *
from parallel.ParallelExecutionPlatforms import ParallelExecutionPlatforms
from plan.IterativeImprovement import IterativeImprovementType, IterativeImprovementInitType
from plan.TreeCostModels import TreeCostModels
from plan.TreePlanBuilderTypes import TreePlanBuilderTypes
from transformation.PatternTransformationRules import PatternTransformationRules
from plan.negation.NegationAlgorithmTypes import NegationAlgorithmTypes
from plan.multi.MultiPatternTreePlanMergeApproaches import MultiPatternTreePlanMergeApproaches

# general settings
DEFAULT_EVALUATION_MECHANISM_TYPE = EvaluationMechanismTypes.TREE_BASED

# plan generation-related defaults
DEFAULT_TREE_PLAN_BUILDER = TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE
DEFAULT_TREE_COST_MODEL = TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL
DEFAULT_TREE_PLAN_MERGE = MultiPatternTreePlanMergeApproaches.TREE_PLAN_SUBTREES_UNION

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

# parallel execution settings
DEFAULT_PARALLEL_EXECUTION_MODE = ParallelExecutionModes.SEQUENTIAL
DEFAULT_PARALLEL_EXECUTION_PLATFORM = ParallelExecutionPlatforms.THREADING
DEFAULT_DATA_PARALLEL_ALGORITHM = DataParallelExecutionModes.RIP_ALGORITHM
DEFAULT_PARALLEL_UNITS_NUMBER = 1
DEFAULT_PARALLEL_KEY = None
DEFAULT_PARALLEL_ATTRIBUTES_DICT = None
DEFAULT_PARALLEL_MULTIPLE = 12

# settings for pattern transformation rules
PREPROCESSING_RULES_ORDER = None  # disabled for now
"""
[
    PatternTransformationRules.AND_AND_PATTERN,
    PatternTransformationRules.NOT_OR_PATTERN,
    PatternTransformationRules.NOT_AND_PATTERN,
    PatternTransformationRules.TOPMOST_OR_PATTERN,
    PatternTransformationRules.INNER_OR_PATTERN,
    PatternTransformationRules.NOT_NOT_PATTERN
]
"""

# default negation algorithm
DEFAULT_NEGATION_ALGORITHM = NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM

# optimizer settings
DEFAULT_OPTIMIZER_TYPE = OptimizerTypes.STATISTICS_DEVIATION_AWARE_OPTIMIZER
DEFAULT_INIT_TREE_PLAN_BUILDER = TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE  # initial tree plan builder in case of predifined statistics
DEVIATION_OPTIMIZER_THRESHOLD = 0.5  # the default threshold for statistics changes aware optimizer
DEFAULT_TREE_UPDATE_TYPE = TreeEvaluationMechanismUpdateTypes.TRIVIAL_TREE_EVALUATION
DEFAULT_STATISTICS_TYPE = [StatisticsTypes.ARRIVAL_RATES, StatisticsTypes.SELECTIVITY_MATRIX]  # the default statistics type can also be a list of types
STATISTICS_TIME_WINDOW = timedelta(hours=1)  # Time window for statistics
STATISTICS_UPDATES_WAIT_TIME = None  # the default wait time between statistics updates or None to disable adaptivity

# Local Search Settings
DEFAULT_SEARCH_TYPE = LocalSearchApproaches.TABU_SEARCH
NEIGHBORHOOD_VERTEX_SIZE = 2  # should be greater than or equal to 2
LOCAL_SEARCH_TIME_LIMIT = 180  # this is in seconds
LOCAL_SEARCH_STEPS_THRESHOLD = 100  # max allowed steps without improvement

SIMULATED_ANNEALING_MULTIPLIER = 0.99  # the decrease rate for the simulated annealing algorithm
SIMULATED_ANNEALING_INIT_NEIGHBORS = 1000  # num of neighbors for simulated annealing init
SIMULATED_ANNEALING_C_THRESHOLD = 0.001  # final threshold to end simulated annealing algorithm

TABU_SEARCH_CAPACITY = 10000  # the capacity of the tabu list
TABU_SEARCH_NEIGHBORHOOD_SIZE = 100  # how many neighbors to search in each step


