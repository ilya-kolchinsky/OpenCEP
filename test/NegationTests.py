from test.testUtils import *
from datetime import timedelta
from condition.Condition import Variable
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import GreaterThanCondition, SmallerThanCondition
from base.PatternStructure import SeqOperator, PrimitiveEventStructure, NegationOperator
from base.Pattern import Pattern
from misc.StatisticsTypes import StatisticsTypes
import copy
from numpy import arange, random



def runAllTrees(pattern, expectedFileName, createTestFile):
    eventsNum = len(pattern.full_structure.get_args())
    # Trivial tree
    origPatt = copy.deepcopy(pattern)
    runTest(expectedFileName, [origPatt], createTestFile, testName="NaiveTrivial")

    # SORT_BY_FREQUENCY_LEFT_DEEP_TREE
    origPatt = copy.deepcopy(pattern)
    origPatt.set_statistics(StatisticsTypes.ARRIVAL_RATES, [*random.rand(eventsNum)])
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest(expectedFileName, [origPatt], createTestFile, eval_params, testName="NaiveSort")

    # GREEDY_LEFT_DEEP_TREE
    origPatt = copy.deepcopy(pattern)
    selectivityMatrix = (random.rand(eventsNum, eventsNum)).tolist()
    arrivalRates = [*random.rand(eventsNum)]
    origPatt.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.GREEDY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest(expectedFileName, [origPatt], createTestFile, eval_params, nasdaqEventStream, testName="NaiveGreedy")


# ON CUSTOM
def multipleNotBeginAndEndTest(createTestFile=False):
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
    runAllTrees(pattern, "MultipleNotBeginAndEnd", createTestFile)
    # runTest("MultipleNotBeginAndEnd", [pattern], createTestFile)

# ON custom2
def simpleNotTest(createTestFile=False):
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
    runAllTrees(pattern, "simpleNot", createTestFile)


# ON NASDAQ SHORT
def multipleNotInTheMiddleTest(createTestFile=False):
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
    # runTest("MultipleNotMiddle", [pattern], createTestFile)
    runAllTrees(pattern, "MultipleNotMiddle", createTestFile)


# ON NASDAQ SHORT
def oneNotAtTheBeginningTest(createTestFile=False):
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
    # runTest("OneNotBegin", [pattern], createTestFile)
    runAllTrees(pattern, "OneNotBegin", createTestFile)

# ON NASDAQ SHORT
def multipleNotAtTheBeginningTest(createTestFile=False):
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
    # runTest("MultipleNotBegin", [pattern], createTestFile)
    runAllTrees(pattern, "MultipleNotBegin", createTestFile)


# ON NASDAQ *HALF* SHORT
def oneNotAtTheEndTest(createTestFile=False):
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
    # runTest("OneNotEnd", [pattern], createTestFile)
    runAllTrees(pattern, "OneNotEnd", createTestFile)


# ON NASDAQ *HALF* SHORT
def multipleNotAtTheEndTest(createTestFile=False):
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
    # runTest("MultipleNotEnd", [pattern], createTestFile)
    runAllTrees(pattern, "MultipleNotEnd", createTestFile)

# ON CUSTOM3
def testWithMultipleNotAtBeginningMiddleEnd(createTestFile=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("AAPL", "a")), PrimitiveEventStructure("AMAZON", "b"), NegationOperator(PrimitiveEventStructure("GOOG", "c")),
                     PrimitiveEventStructure("FB", "d"), NegationOperator(PrimitiveEventStructure("TYP1", "x"))),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    # runTest("NotEverywhere", [pattern], createTestFile)
    runAllTrees(pattern, "NotEverywhere", createTestFile)
