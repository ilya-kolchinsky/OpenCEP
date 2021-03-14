from adaptive.optimizer.OptimizerFactory import OptimizerParameters
from adaptive.optimizer.OptimizerTypes import OptimizerTypes
from adaptive.statistics.StatisticsTypes import StatisticsTypes
from test.testUtils import *
from datetime import timedelta
from condition.Condition import Variable
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import GreaterThanCondition, SmallerThanCondition
from base.PatternStructure import SeqOperator, PrimitiveEventStructure, NegationOperator
from base.Pattern import Pattern
import random
from plan.negation.NegationAlgorithmTypes import NegationAlgorithmTypes


def generate_statistics(events_num: int):
    arrival_rates = [random.random() for _ in range(events_num)]
    selectivity_matrix = [[random.random() for _ in range(events_num)] for _ in range(events_num)]
    for i in range(1, events_num):
        for j in range(i):
            selectivity_matrix[i][j] = selectivity_matrix[j][i]
    for i in range(events_num):
        selectivity_matrix[i][i] = 1.0
    return {StatisticsTypes.SELECTIVITY_MATRIX: selectivity_matrix, StatisticsTypes.ARRIVAL_RATES: arrival_rates}


# ON CUSTOM
def multipleNotBeginAndEndTest(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("TYP1", "x")),
                    NegationOperator(PrimitiveEventStructure("TYP4", "t")),
                    PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    NegationOperator(PrimitiveEventStructure("TYP2", "y")),
                    NegationOperator(PrimitiveEventStructure("TYP3", "z"))),
        AndCondition(
            GreaterThanCondition(Variable("x", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("y", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("t", lambda x: x["Opening Price"]),
                                 Variable("a", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)))
    runTest("MultipleNotBeginAndEnd", [pattern], create_test_file, eval_params)


# ON custom2
def simpleNotTest(create_test_file=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), NegationOperator(PrimitiveEventStructure("AMZN", "b")), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)))
    runTest("simpleNot", [pattern], create_test_file, eval_params)


# ON NASDAQ SHORT
def multipleNotInTheMiddleTest(create_test_file=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), NegationOperator(PrimitiveEventStructure("LI", "d")), PrimitiveEventStructure("AMZN", "b"),
                     NegationOperator(PrimitiveEventStructure("FB", "e")), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
                GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                     Variable("b", lambda x: x["Opening Price"])),
                SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                     Variable("c", lambda x: x["Opening Price"])),
                GreaterThanCondition(Variable("e", lambda x: x["Opening Price"]),
                                     Variable("a", lambda x: x["Opening Price"])),
                SmallerThanCondition(Variable("d", lambda x: x["Opening Price"]),
                                     Variable("c", lambda x: x["Opening Price"]))
            ),
        timedelta(minutes=4)
    )
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)))
    runTest("MultipleNotMiddle", [pattern], create_test_file, eval_params)


# ON NASDAQ SHORT
def oneNotAtTheBeginningTest(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("TYP1", "x")), PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)))
    runTest("OneNotBegin", [pattern], create_test_file, eval_params)


# ON NASDAQ SHORT
def multipleNotAtTheBeginningTest(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("TYP1", "x")), NegationOperator(PrimitiveEventStructure("TYP2", "y")),
                    NegationOperator(PrimitiveEventStructure("TYP3", "z")), PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)))
    runTest("MultipleNotBegin", [pattern], create_test_file, eval_params)


# ON NASDAQ *HALF* SHORT
def oneNotAtTheEndTest(create_test_file=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c"), NegationOperator(PrimitiveEventStructure("TYP1", "x"))),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)))
    runTest("OneNotEnd", [pattern], create_test_file, eval_params)


# ON NASDAQ *HALF* SHORT
def multipleNotAtTheEndTest(create_test_file=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c"), NegationOperator(PrimitiveEventStructure("TYP1", "x")),
                    NegationOperator(PrimitiveEventStructure("TYP2", "y")), NegationOperator(PrimitiveEventStructure("TYP3", "z"))),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)))
    runTest("MultipleNotEnd", [pattern], create_test_file, eval_params)


# ON CUSTOM3
def testWithMultipleNotAtBeginningMiddleEnd(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("AAPL", "a")), PrimitiveEventStructure("AMAZON", "b"),
                    NegationOperator(PrimitiveEventStructure("GOOG", "c")), PrimitiveEventStructure("FB", "d"),
                    NegationOperator(PrimitiveEventStructure("TYP1", "x"))),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)))
    runTest("NotEverywhere", [pattern], create_test_file, eval_params)


# ON CUSTOM
def testWithMultipleNotAtBeginningMiddleEnd2(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("TYP1", "x")),
                    NegationOperator(PrimitiveEventStructure("TYP4", "t")),
                    PrimitiveEventStructure("AAPL", "a"),
                    NegationOperator(PrimitiveEventStructure("TYP5", "m")),
                    NegationOperator(PrimitiveEventStructure("TYP6", "f")),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    NegationOperator(PrimitiveEventStructure("TYP2", "y")),
                    NegationOperator(PrimitiveEventStructure("TYP3", "z"))),
        AndCondition(
            GreaterThanCondition(Variable("x", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("y", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("t", lambda x: x["Opening Price"]),
                                 Variable("a", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)))
    runTest("NotEverywhere2", [pattern], create_test_file, eval_params)


# ON CUSTOM
def multipleNotBeginAndEndTestStat(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("TYP1", "x")),
                    NegationOperator(PrimitiveEventStructure("TYP4", "t")),
                    PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    NegationOperator(PrimitiveEventStructure("TYP2", "y")),
                    NegationOperator(PrimitiveEventStructure("TYP3", "z"))),
        AndCondition(
            GreaterThanCondition(Variable("x", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("y", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("t", lambda x: x["Opening Price"]),
                                 Variable("a", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)))
    runTest("MultipleNotBeginAndEndStat", [pattern], create_test_file, eval_params, expected_file_name="MultipleNotBeginAndEnd")


# ON custom2
def simpleNotTestStat(create_test_file=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), NegationOperator(PrimitiveEventStructure("AMZN", "b")), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)))
    runTest("simpleNotStat", [pattern], create_test_file, eval_params, expected_file_name="simpleNot")


# ON NASDAQ SHORT
def multipleNotInTheMiddleTestStat(create_test_file=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), NegationOperator(PrimitiveEventStructure("LI", "d")), PrimitiveEventStructure("AMZN", "b"),
                     NegationOperator(PrimitiveEventStructure("FB", "e")), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
                GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                     Variable("b", lambda x: x["Opening Price"])),
                SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                     Variable("c", lambda x: x["Opening Price"])),
                GreaterThanCondition(Variable("e", lambda x: x["Opening Price"]),
                                     Variable("a", lambda x: x["Opening Price"])),
                SmallerThanCondition(Variable("d", lambda x: x["Opening Price"]),
                                     Variable("c", lambda x: x["Opening Price"]))
            ),
        timedelta(minutes=4)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)))
    runTest("MultipleNotMiddleStat", [pattern], create_test_file, eval_params, expected_file_name="MultipleNotMiddle")


# ON NASDAQ SHORT
def oneNotAtTheBeginningTestStat(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("TYP1", "x")), PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)))
    runTest("OneNotBeginStat", [pattern], create_test_file, eval_params, expected_file_name="OneNotBegin")


# ON NASDAQ SHORT
def multipleNotAtTheBeginningTestStat(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("TYP1", "x")), NegationOperator(PrimitiveEventStructure("TYP2", "y")),
                    NegationOperator(PrimitiveEventStructure("TYP3", "z")), PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)))
    runTest("MultipleNotBeginStat", [pattern], create_test_file, eval_params, expected_file_name="MultipleNotBegin")


# ON NASDAQ *HALF* SHORT
def oneNotAtTheEndTestStat(create_test_file=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c"), NegationOperator(PrimitiveEventStructure("TYP1", "x"))),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)))
    runTest("OneNotEndStat", [pattern], create_test_file, eval_params, expected_file_name="OneNotEnd")


# ON NASDAQ *HALF* SHORT
def multipleNotAtTheEndTestStat(create_test_file=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c"), NegationOperator(PrimitiveEventStructure("TYP1", "x")),
                    NegationOperator(PrimitiveEventStructure("TYP2", "y")), NegationOperator(PrimitiveEventStructure("TYP3", "z"))),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)))
    runTest("MultipleNotEndStat", [pattern], create_test_file, eval_params, expected_file_name="MultipleNotEnd")


# ON CUSTOM3
def testWithMultipleNotAtBeginningMiddleEndStat(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("AAPL", "a")), PrimitiveEventStructure("AMAZON", "b"),
                    NegationOperator(PrimitiveEventStructure("GOOG", "c")), PrimitiveEventStructure("FB", "d"),
                    NegationOperator(PrimitiveEventStructure("TYP1", "x"))),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)))
    runTest("NotEverywhereStat", [pattern], create_test_file, eval_params, expected_file_name="NotEverywhere")


# ON CUSTOM
def testWithMultipleNotAtBeginningMiddleEnd2Stat(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("TYP1", "x")),
                    NegationOperator(PrimitiveEventStructure("TYP4", "t")),
                    PrimitiveEventStructure("AAPL", "a"),
                    NegationOperator(PrimitiveEventStructure("TYP5", "m")),
                    NegationOperator(PrimitiveEventStructure("TYP6", "f")),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    NegationOperator(PrimitiveEventStructure("TYP2", "y")),
                    NegationOperator(PrimitiveEventStructure("TYP3", "z"))),
        AndCondition(
            GreaterThanCondition(Variable("x", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("y", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("t", lambda x: x["Opening Price"]),
                                 Variable("a", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)))
    runTest("NotEverywhere2Stat", [pattern], create_test_file, eval_params, expected_file_name="NotEverywhere2")


# ON CUSTOM
def multipleNotBeginAndEndTestDPTree(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("TYP1", "x")),
                    NegationOperator(PrimitiveEventStructure("TYP4", "t")),
                    PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    NegationOperator(PrimitiveEventStructure("TYP2", "y")),
                    NegationOperator(PrimitiveEventStructure("TYP3", "z"))),
        AndCondition(
            GreaterThanCondition(Variable("x", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("y", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("t", lambda x: x["Opening Price"]),
                                 Variable("a", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)))
    runTest("MultipleNotBeginAndEndDPTree", [pattern], create_test_file, eval_params, expected_file_name="MultipleNotBeginAndEnd")


# ON custom2
def simpleNotTestDPTree(create_test_file=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), NegationOperator(PrimitiveEventStructure("AMZN", "b")), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)))
    runTest("simpleNotDPTree", [pattern], create_test_file, eval_params, expected_file_name="simpleNot")


# ON NASDAQ SHORT
def multipleNotInTheMiddleTestDPTree(create_test_file=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), NegationOperator(PrimitiveEventStructure("LI", "d")), PrimitiveEventStructure("AMZN", "b"),
                     NegationOperator(PrimitiveEventStructure("FB", "e")), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
                GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                     Variable("b", lambda x: x["Opening Price"])),
                SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                     Variable("c", lambda x: x["Opening Price"])),
                GreaterThanCondition(Variable("e", lambda x: x["Opening Price"]),
                                     Variable("a", lambda x: x["Opening Price"])),
                SmallerThanCondition(Variable("d", lambda x: x["Opening Price"]),
                                     Variable("c", lambda x: x["Opening Price"]))
            ),
        timedelta(minutes=4)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)))
    runTest("MultipleNotMiddleDPTree", [pattern], create_test_file, eval_params, expected_file_name="MultipleNotMiddle")


# ON NASDAQ SHORT
def oneNotAtTheBeginningTestDPTree(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("TYP1", "x")), PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)))
    runTest("OneNotBeginDPTree", [pattern], create_test_file, eval_params, expected_file_name="OneNotBegin")


# ON NASDAQ SHORT
def multipleNotAtTheBeginningTestDPTree(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("TYP1", "x")), NegationOperator(PrimitiveEventStructure("TYP2", "y")),
                    NegationOperator(PrimitiveEventStructure("TYP3", "z")), PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)))
    runTest("MultipleNotBeginDPTree", [pattern], create_test_file, eval_params, expected_file_name="MultipleNotBegin")


# ON NASDAQ *HALF* SHORT
def oneNotAtTheEndTestDPTree(create_test_file=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c"), NegationOperator(PrimitiveEventStructure("TYP1", "x"))),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)))
    runTest("OneNotEndDPTree", [pattern], create_test_file, eval_params, expected_file_name="OneNotEnd")


# ON NASDAQ *HALF* SHORT
def multipleNotAtTheEndTestDPTree(create_test_file=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c"), NegationOperator(PrimitiveEventStructure("TYP1", "x")),
                    NegationOperator(PrimitiveEventStructure("TYP2", "y")), NegationOperator(PrimitiveEventStructure("TYP3", "z"))),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)))
    runTest("MultipleNotEndDPTree", [pattern], create_test_file, eval_params, expected_file_name="MultipleNotEnd")


# ON CUSTOM3
def testWithMultipleNotAtBeginningMiddleEndDPTree(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("AAPL", "a")), PrimitiveEventStructure("AMAZON", "b"),
                    NegationOperator(PrimitiveEventStructure("GOOG", "c")), PrimitiveEventStructure("FB", "d"),
                    NegationOperator(PrimitiveEventStructure("TYP1", "x"))),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)))
    runTest("NotEverywhereDPTree", [pattern], create_test_file, eval_params, expected_file_name="NotEverywhere")


# ON CUSTOM
def testWithMultipleNotAtBeginningMiddleEnd2DPTree(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("TYP1", "x")),
                    NegationOperator(PrimitiveEventStructure("TYP4", "t")),
                    PrimitiveEventStructure("AAPL", "a"),
                    NegationOperator(PrimitiveEventStructure("TYP5", "m")),
                    NegationOperator(PrimitiveEventStructure("TYP6", "f")),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    NegationOperator(PrimitiveEventStructure("TYP2", "y")),
                    NegationOperator(PrimitiveEventStructure("TYP3", "z"))),
        AndCondition(
            GreaterThanCondition(Variable("x", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("y", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("t", lambda x: x["Opening Price"]),
                                 Variable("a", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)))
    runTest("NotEverywhere2DPTree", [pattern], create_test_file, eval_params, expected_file_name="NotEverywhere2")


# ON CUSTOM
def multipleNotBeginAndEndTestStatDPTree(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("TYP1", "x")),
                    NegationOperator(PrimitiveEventStructure("TYP4", "t")),
                    PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    NegationOperator(PrimitiveEventStructure("TYP2", "y")),
                    NegationOperator(PrimitiveEventStructure("TYP3", "z"))),
        AndCondition(
            GreaterThanCondition(Variable("x", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("y", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("t", lambda x: x["Opening Price"]),
                                 Variable("a", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)))
    runTest("MultipleNotBeginAndEndStatDPTree", [pattern], create_test_file, eval_params, expected_file_name="MultipleNotBeginAndEnd")


# ON custom2
def simpleNotTestStatDPTree(create_test_file=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), NegationOperator(PrimitiveEventStructure("AMZN", "b")), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)))
    runTest("simpleNotStatDPTree", [pattern], create_test_file, eval_params, expected_file_name="simpleNot")


# ON NASDAQ SHORT
def multipleNotInTheMiddleTestStatDPTree(create_test_file=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), NegationOperator(PrimitiveEventStructure("LI", "d")), PrimitiveEventStructure("AMZN", "b"),
                     NegationOperator(PrimitiveEventStructure("FB", "e")), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
                GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                     Variable("b", lambda x: x["Opening Price"])),
                SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                     Variable("c", lambda x: x["Opening Price"])),
                GreaterThanCondition(Variable("e", lambda x: x["Opening Price"]),
                                     Variable("a", lambda x: x["Opening Price"])),
                SmallerThanCondition(Variable("d", lambda x: x["Opening Price"]),
                                     Variable("c", lambda x: x["Opening Price"]))
            ),
        timedelta(minutes=4)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)))
    runTest("MultipleNotMiddleStatDPTree", [pattern], create_test_file, eval_params, expected_file_name="MultipleNotMiddle")


# ON NASDAQ SHORT
def oneNotAtTheBeginningTestStatDPTree(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("TYP1", "x")), PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)))
    runTest("OneNotBeginStatDPTree", [pattern], create_test_file, eval_params, expected_file_name="OneNotBegin")


# ON NASDAQ SHORT
def multipleNotAtTheBeginningTestStatDPTree(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("TYP1", "x")), NegationOperator(PrimitiveEventStructure("TYP2", "y")),
                    NegationOperator(PrimitiveEventStructure("TYP3", "z")), PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)))
    runTest("MultipleNotBeginStatDPTree", [pattern], create_test_file, eval_params, expected_file_name="MultipleNotBegin")


# ON NASDAQ *HALF* SHORT
def oneNotAtTheEndTestStatDPTree(create_test_file=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c"), NegationOperator(PrimitiveEventStructure("TYP1", "x"))),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)))
    runTest("OneNotEndStatDPTree", [pattern], create_test_file, eval_params, expected_file_name="OneNotEnd")


# ON NASDAQ *HALF* SHORT
def multipleNotAtTheEndTestStatDPTree(create_test_file=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c"), NegationOperator(PrimitiveEventStructure("TYP1", "x")),
                    NegationOperator(PrimitiveEventStructure("TYP2", "y")), NegationOperator(PrimitiveEventStructure("TYP3", "z"))),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)))
    runTest("MultipleNotEndStatDPTree", [pattern], create_test_file, eval_params, expected_file_name="MultipleNotEnd")


# ON CUSTOM3
def testWithMultipleNotAtBeginningMiddleEndStatDPTree(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("AAPL", "a")), PrimitiveEventStructure("AMAZON", "b"),
                    NegationOperator(PrimitiveEventStructure("GOOG", "c")), PrimitiveEventStructure("FB", "d"),
                    NegationOperator(PrimitiveEventStructure("TYP1", "x"))),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)))
    runTest("NotEverywhereStatDPTree", [pattern], create_test_file, eval_params, expected_file_name="NotEverywhere")


# ON CUSTOM
def testWithMultipleNotAtBeginningMiddleEnd2StatDPTree(create_test_file=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("TYP1", "x")),
                    NegationOperator(PrimitiveEventStructure("TYP4", "t")),
                    PrimitiveEventStructure("AAPL", "a"),
                    NegationOperator(PrimitiveEventStructure("TYP5", "m")),
                    NegationOperator(PrimitiveEventStructure("TYP6", "f")),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    NegationOperator(PrimitiveEventStructure("TYP2", "y")),
                    NegationOperator(PrimitiveEventStructure("TYP3", "z"))),
        AndCondition(
            GreaterThanCondition(Variable("x", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("y", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("t", lambda x: x["Opening Price"]),
                                 Variable("a", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(generate_statistics(pattern.count_primitive_events()))
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(builder_type=TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
                                  negation_algorithm_type=NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)))
    runTest("NotEverywhere2StatDPTree", [pattern], create_test_file, eval_params, expected_file_name="NotEverywhere2")
