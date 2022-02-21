from random import random

from adaptive.optimizer.OptimizerFactory import OptimizerParameters
from adaptive.optimizer.OptimizerTypes import OptimizerTypes
from adaptive.statistics.StatisticsCollectorFactory import StatisticsCollectorParameters
from adaptive.statistics.StatisticsTypes import StatisticsTypes
from plan.multi.MultiPatternTreePlanMergeApproaches import MultiPatternTreePlanMergeApproaches
from plan.multi.local_search.LocalSearchFactory import TabuSearchLocalSearchParameters,\
    SimulatedAnnealingLocalSearchParameters
from test.testUtils import *
from datetime import timedelta
from condition.Condition import Variable
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import GreaterThanCondition, SmallerThanCondition
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure
from base.Pattern import Pattern


TABU_SEARCH_LOCAL_EVALUATION_MECHANISM_SETTINGS = \
    TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(
                                             statistics_collector_params=StatisticsCollectorParameters(
                                                             statistics_types=[StatisticsTypes.ARRIVAL_RATES]),
                                             opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(
                                                 builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                                 cost_model_type=TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL,
                                                 tree_plan_merger_type=MultiPatternTreePlanMergeApproaches.TREE_PLAN_LOCAL_SEARCH)),
        storage_params=TreeStorageParameters(sort_storage=False, clean_up_interval=10,
                                             prioritize_sorting_by_timestamp=True),
        local_search_params=TabuSearchLocalSearchParameters(
            neighborhood_vertex_size=2, time_limit=10, steps_threshold=100,
            capacity=10000, neighborhood_size=100))

SIMULATED_ANNEALING_LOCAL_EVALUATION_MECHANISM_SETTINGS = \
    TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(
                                             statistics_collector_params=StatisticsCollectorParameters(
                                                             statistics_types=[StatisticsTypes.ARRIVAL_RATES]),
                                             opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(
                                                 builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                                 cost_model_type=TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL,
                                                 tree_plan_merger_type=MultiPatternTreePlanMergeApproaches.TREE_PLAN_LOCAL_SEARCH)),
        storage_params=TreeStorageParameters(sort_storage=False, clean_up_interval=10,
                                             prioritize_sorting_by_timestamp=True),
        local_search_params=SimulatedAnnealingLocalSearchParameters(
            neighborhood_vertex_size=2, time_limit=10, steps_threshold=100, initial_neighbors=1000, multiplier=0.99,
            simulated_annealing_threshold=0.001))

SIMULATED_ANNEALING_BUSHY_LOCAL_EVALUATION_MECHANISM_SETTINGS = \
    TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(
                                             statistics_collector_params=StatisticsCollectorParameters(
                                                             statistics_types=[StatisticsTypes.ARRIVAL_RATES]),
                                             opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(
                                                 builder_type=TreePlanBuilderTypes.ZSTREAM_BUSHY_TREE,
                                                 cost_model_type=TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL,
                                                 tree_plan_merger_type=MultiPatternTreePlanMergeApproaches.TREE_PLAN_LOCAL_SEARCH)),
        storage_params=TreeStorageParameters(sort_storage=False, clean_up_interval=10,
                                             prioritize_sorting_by_timestamp=True),
        local_search_params=SimulatedAnnealingLocalSearchParameters(
            neighborhood_vertex_size=2, time_limit=10, steps_threshold=100, initial_neighbors=1000, multiplier=0.99,
            simulated_annealing_threshold=0.001))


def create_statistics(patterns):
    """
    Generate statistics for patterns
    """
    for pattern in patterns:
        statistics = dict()
        num_of_events = pattern.count_primitive_events()
        arrival_rates = [random() for x in range(num_of_events)]
        selectivity = [[random() for x in range(num_of_events)] for y in range(num_of_events)]
        # fix matrix: M[i][i] = 1.0 and M[i][j] = M[j][i] for each x,y
        for i in range(1, num_of_events):
            for j in range(i):
                selectivity[i][j] = selectivity[j][i]
        for i in range(num_of_events):
            selectivity[i][i] = 1.0

        statistics[StatisticsTypes.ARRIVAL_RATES] = arrival_rates
        statistics[StatisticsTypes.SELECTIVITY_MATRIX] = selectivity
        pattern.set_statistics(statistics)


"""
local tabu search test 2 disjoint patterns
"""


def localTabuSearchDisjoint(createTestFile=False, eval_mechanism_params=TABU_SEARCH_LOCAL_EVALUATION_MECHANISM_SETTINGS):

    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    create_statistics([pattern1, pattern2])

    runTest("DisjointLocalTabuSearch", [pattern1, pattern2], createTestFile, eval_mechanism_params)


"""
local simulated search test 2 disjoint patterns
"""


def localSimulatedSearchDisjoint(createTestFile=False, eval_mechanism_params=SIMULATED_ANNEALING_LOCAL_EVALUATION_MECHANISM_SETTINGS):

    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    create_statistics([pattern1, pattern2])

    runTest("DisjointLocalSASearch", [pattern1, pattern2], createTestFile, eval_mechanism_params)


"""
local tabu search test one event sharing
"""


def localTabuSearchLeafSharing(createTestFile=False, eval_mechanism_params=TABU_SEARCH_LOCAL_EVALUATION_MECHANISM_SETTINGS):

    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    create_statistics([pattern1, pattern2])

    runTest("OneEventLocalTabuSearch", [pattern1, pattern2], createTestFile, eval_mechanism_params)


"""
local simulated search test one event sharing
"""


def localSimulatedSearchLeafSharing(createTestFile=False, eval_mechanism_params=SIMULATED_ANNEALING_LOCAL_EVALUATION_MECHANISM_SETTINGS):

    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    create_statistics([pattern1, pattern2])

    runTest("OneEventLocalSASearch", [pattern1, pattern2], createTestFile, eval_mechanism_params)


"""
local tabu search test two events sharing
"""


def localTabuSearchMultiSharing(createTestFile=False, eval_mechanism_params=TABU_SEARCH_LOCAL_EVALUATION_MECHANISM_SETTINGS):

    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
        AndCondition(GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
                     SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]), 120)),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
                     SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]), 120),
                     SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    create_statistics([pattern1, pattern2])

    runTest("DoubleEventLocalTabuSearch", [pattern1, pattern2], createTestFile, eval_mechanism_params)


"""
local simulated search test two events sharing
"""


def localSimulatedSearchMultiSharing(createTestFile=False, eval_mechanism_params=SIMULATED_ANNEALING_LOCAL_EVALUATION_MECHANISM_SETTINGS):

    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
        AndCondition(GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
                     SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]), 120)),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
                     SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]), 120),
                     SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    create_statistics([pattern1, pattern2])

    runTest("DoubleEventSASearch", [pattern1, pattern2], createTestFile, eval_mechanism_params)


"""
local tabu search test 3 patterns, only two share
"""


def localTabuSearchTriplePatterns(createTestFile=False, eval_mechanism_params=TABU_SEARCH_LOCAL_EVALUATION_MECHANISM_SETTINGS):

    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(SeqOperator(PrimitiveEventStructure("AAPL", "a")), PrimitiveEventStructure("AMZN", "b")),
        AndCondition(GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
                     SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]), 120)),
        timedelta(minutes=5)
    )

    pattern3 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "c")),
        GreaterThanCondition(Variable("c", lambda x: x["Opening Price"]), 530.1),
        timedelta(minutes=5)
    )
    create_statistics([pattern1, pattern2, pattern3])

    runTest("TriplePatternLocalTabuSearch", [pattern1, pattern2, pattern3], createTestFile, eval_mechanism_params)


"""
local simulated search test 3 patterns, only two share
"""


def localSimulatedSearchTriplePatterns(createTestFile=False, eval_mechanism_params=SIMULATED_ANNEALING_LOCAL_EVALUATION_MECHANISM_SETTINGS):

    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
        AndCondition(GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
                     SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]), 120)),
        timedelta(minutes=5)
    )

    pattern3 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "c")),
        GreaterThanCondition(Variable("c", lambda x: x["Opening Price"]), 530.1),
        timedelta(minutes=5)
    )
    create_statistics([pattern1, pattern2, pattern3])

    runTest("TriplePatternLocalSASearch", [pattern1, pattern2, pattern3], createTestFile, eval_mechanism_params)


"""
local tabu search test 3 patterns, all three share sub tree
"""


def localTabuSearchTripleSharePatterns(createTestFile=False, eval_mechanism_params=TABU_SEARCH_LOCAL_EVALUATION_MECHANISM_SETTINGS):

    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
        AndCondition(GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
                     SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]), 120)),
        timedelta(minutes=5)
    )

    pattern3 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
                     SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]), 120),
                     GreaterThanCondition(Variable("c", lambda x: x["Opening Price"]), 530.1)),
        timedelta(minutes=5)
    )
    create_statistics([pattern1, pattern2, pattern3])

    runTest("TripleSharePatternLocalTabuSearch", [pattern1, pattern2, pattern3], createTestFile, eval_mechanism_params)


"""
local simulated search test 3 patterns, all three share sub tree
"""


def locaSimulatedSearchTripleSharePatterns(createTestFile=False, eval_mechanism_params=SIMULATED_ANNEALING_LOCAL_EVALUATION_MECHANISM_SETTINGS):

    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
        AndCondition(GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
                     SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]), 120)),
        timedelta(minutes=5)
    )

    pattern3 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
                     SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]), 120),
                     GreaterThanCondition(Variable("c", lambda x: x["Opening Price"]), 530.1)),
        timedelta(minutes=5)
    )
    create_statistics([pattern1, pattern2, pattern3])

    runTest("TripleSharePatternSASearch", [pattern1, pattern2, pattern3], createTestFile, eval_mechanism_params)


"""
local tabu search test AndOperator between 2 patterns
"""


def localTabuSearchAndPatterns(createTestFile=False, eval_mechanism_params=TABU_SEARCH_LOCAL_EVALUATION_MECHANISM_SETTINGS):

    pattern1 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
        AndCondition(GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
                     SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]), 79.2)),
        timedelta(minutes=5)
    )
    create_statistics([pattern1, pattern2])

    runTest("AndPatternLocalTabuSearch", [pattern1, pattern2], createTestFile, eval_mechanism_params)


"""
local simulated search test AndOperator between 2 patterns
"""


def localSimulatedSearchAndPatterns(createTestFile=False, eval_mechanism_params=SIMULATED_ANNEALING_LOCAL_EVALUATION_MECHANISM_SETTINGS):

    pattern1 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
        AndCondition(GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
                     SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]), 79.2)),
        timedelta(minutes=5)
    )
    create_statistics([pattern1, pattern2])

    runTest("AndPatternLocalSASearch", [pattern1, pattern2], createTestFile, eval_mechanism_params)


"""
local tabu search test AndOperator opposite direction
"""


def localTabuSearchAndOpposite(createTestFile=False, eval_mechanism_params=TABU_SEARCH_LOCAL_EVALUATION_MECHANISM_SETTINGS):

    pattern1 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        AndOperator(PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AAPL", "a")),
        AndCondition(GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
                     SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]), 79.2)),
        timedelta(minutes=5)
    )
    create_statistics([pattern1, pattern2])

    runTest("AndOppositeLocalTabuSearch", [pattern1, pattern2], createTestFile, eval_mechanism_params)


"""
local simulated search test AndOperator opposite direction
"""


def localSimulatedSearchAndOpposite(createTestFile=False, eval_mechanism_params=SIMULATED_ANNEALING_LOCAL_EVALUATION_MECHANISM_SETTINGS):

    pattern1 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        AndOperator(PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AAPL", "a")),
        AndCondition(GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
                     SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]), 79.2)),
        timedelta(minutes=5)
    )
    create_statistics([pattern1, pattern2])

    runTest("AndOppositeLocalSASearch", [pattern1, pattern2], createTestFile, eval_mechanism_params)


"""
local simulated annealing search test with bushy tree
"""


def localSimulatedBushySearchAndOpposite(createTestFile=False, eval_mechanism_params=SIMULATED_ANNEALING_BUSHY_LOCAL_EVALUATION_MECHANISM_SETTINGS):

    pattern1 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        AndOperator(PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AAPL", "a")),
        AndCondition(GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
                     SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]), 79.2)),
        timedelta(minutes=5)
    )
    create_statistics([pattern1, pattern2])

    runTest("AndOppositeBushyLocalSASearch", [pattern1, pattern2], createTestFile, eval_mechanism_params)