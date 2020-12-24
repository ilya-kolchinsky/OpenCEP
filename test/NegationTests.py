from test.testUtils import *
from datetime import timedelta
from condition.Condition import Variable
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import GreaterThanCondition, SmallerThanCondition
from base.PatternStructure import SeqOperator, PrimitiveEventStructure, NegationOperator
from base.Pattern import Pattern
from misc.StatisticsTypes import StatisticsTypes
import copy
from numpy import random, fill_diagonal

"""
This function creates random statistics for the negation tests including based statistics trees
The generated statistics are documented at "test/StatisticsDocumentation/statistics.txt" 
"""
def setStatisticsForTest(eventsNum , expectedFileName , negationAlgo):
    arrivalRates = [*random.rand(eventsNum)]
    selectivityMatrixA = random.rand(eventsNum, eventsNum)
    selectivityMatrix = selectivityMatrixA.T * selectivityMatrixA
    fill_diagonal(selectivityMatrix, 1)
    selectivityMatrix = selectivityMatrix.tolist()
    with open(os.path.join(absolutePath, 'test/StatisticsDocumentation/statistics.txt'), 'a') as file:
        file.write("\nStatistics values in %s %s tests: \n\n" % (expectedFileName , negationAlgo))
        file.write("arrivalRates: [ ")
        for i in arrivalRates:
            file.write(str(i) + " ")
        file.write("]" + "\n")
        file.write("selectivityMatrix:" + "\n")
        for line in selectivityMatrix:
            file.write("[")
            for i in line:
                file.write(str(i) + " ")
            file.write("]" + "\n")
        file.write("\n**********************\n")
    file.close()
    return selectivityMatrix, arrivalRates

def runAllTrees(pattern, expectedFileName, createTestFile, negationAlgo=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM):
    eventsNum = len(pattern.full_structure.get_args())
    selectivityMatrix, arrivalRates = setStatisticsForTest(eventsNum, expectedFileName, negationAlgo)

    # print("arrival rates are: ")
    # print(*arrivalRates, sep=", ")
    # print("selectivity matrix is: ")
    # for s in selectivityMatrix:
      #  print(*s)

    # Trivial tree
    origPatt = copy.deepcopy(pattern)
    origPatt.set_statistics(StatisticsTypes.ARRIVAL_RATES, arrivalRates)
    runTest(expectedFileName, [origPatt], createTestFile, negation_algorithm=negationAlgo, testName="Trivial")

    # SORT_BY_FREQUENCY_LEFT_DEEP_TREE
    origPatt = copy.deepcopy(pattern)
    origPatt.set_statistics(StatisticsTypes.ARRIVAL_RATES, arrivalRates)
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest(expectedFileName, [origPatt], createTestFile, eval_params, negation_algorithm=negationAlgo, testName="Sort")

    # GREEDY_LEFT_DEEP_TREE
    origPatt = copy.deepcopy(pattern)
    origPatt.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.GREEDY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest(expectedFileName, [origPatt], createTestFile, eval_params, nasdaqEventStream,
            negation_algorithm=negationAlgo, testName="Greedy")

    # LOCAL_SEARCH_LEFT_DEEP_TREE
    # this tree was not checked yet (at all)

    # DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE
    origPatt = copy.deepcopy(pattern)
    origPatt.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest(expectedFileName, [origPatt], createTestFile, eval_params, nasdaqEventStream,
            negation_algorithm=negationAlgo, testName="DP")

    # DYNAMIC_PROGRAMMING_BUSHY_TREE
    origPatt = copy.deepcopy(pattern)
    origPatt.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest(expectedFileName, [origPatt], createTestFile, eval_params, nasdaqEventStream,
            negation_algorithm=negationAlgo, testName="DPBushy")

    # ZSTREAM_BUSHY_TREE
    origPatt = copy.deepcopy(pattern)
    origPatt.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.ORDERED_ZSTREAM_BUSHY_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest(expectedFileName, [origPatt], createTestFile, eval_params, nasdaqEventStream,
            negation_algorithm=negationAlgo, testName="ZSBushy")

    # ORDERED_ZSTREAM_BUSHY_TREE
    origPatt = copy.deepcopy(pattern)
    origPatt.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.ORDERED_ZSTREAM_BUSHY_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest(expectedFileName, [origPatt], createTestFile, eval_params, nasdaqEventStream,
            negation_algorithm=negationAlgo, testName="OrderedZSBushy")


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
    # runAllTrees(pattern, "MultipleNotBeginAndEnd", createTestFile)
    runAllTrees(pattern, "MultipleNotBeginAndEnd", createTestFile, NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)
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
    runAllTrees(pattern, "simpleNot", createTestFile, NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)


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
    runAllTrees(pattern, "MultipleNotMiddle", createTestFile, NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)


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
    runAllTrees(pattern, "OneNotBegin", createTestFile, NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)

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
    runAllTrees(pattern, "MultipleNotBegin", createTestFile, NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)


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
    runAllTrees(pattern, "OneNotEnd", createTestFile, NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)


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
    runAllTrees(pattern, "MultipleNotEnd", createTestFile, NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)

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
    runAllTrees(pattern, "NotEverywhere", createTestFile, NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)
