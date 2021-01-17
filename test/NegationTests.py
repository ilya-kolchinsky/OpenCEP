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
from itertools import permutations


class Statistics:
    def __init__(self, pattern, path):
        self.path = path
        self.negation_index_list = []
        self.map_comb = {} # key: negative events' indices combination, value : 0  or 1 (if the combination was checked).
        self.comb_done = 0
        self.last_comb = ""
        self.eventsNum = len(pattern.full_structure.get_args())
        # Collecting the negative events' indices
        for i, arg in enumerate(pattern.full_structure.get_args()):
            if type(arg) == NegationOperator:
                self.negation_index_list.append(i)
        # Generating all the possible combinations for the negative events.
        for val in list(permutations(self.negation_index_list)):
            self.map_comb[val] = 0

    def setStatisticsForTest(self, expectedFileName):
        """
        The function searches for statistic with negative indexes combination that wasn't checked before.
        If it succeeds, it returns the statistics, otherwise it returns None.
        """
        eventsNum = self.eventsNum
        arrivalRates = [*random.rand(eventsNum)]
        counter = 0
        # Maximum searching loops for a new combination
        max_loops = 5
        is_valid = self.is_statistic_valid(arrivalRates)
        while counter < max_loops and is_valid is False:
            arrivalRates = [*random.rand(eventsNum)]
            counter = counter + 1
            is_valid = self.is_statistic_valid(arrivalRates)
        if counter == max_loops and is_valid is False:
            return None, None, False
        selectivityMatrixA = random.rand(eventsNum, eventsNum)
        selectivityMatrix = selectivityMatrixA.T * selectivityMatrixA
        fill_diagonal(selectivityMatrix, 1)
        selectivityMatrix = selectivityMatrix.tolist()

        # Documentation of the new statistics.
        with open(self.path, 'a') as file:
            file.write("\nStatistics values in %s tests: \n\n" % expectedFileName)
            file.write("negation combination: %s \n" % self.last_comb)
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
        return selectivityMatrix, arrivalRates, True

    def is_statistic_valid(self, arrivalRates):
        """
        The function returns True if a statistic with the same negative index combination wasn't checked before.
        """
        map_neg_statistic_comb = {}
        # Creating a map with a negative event index as a key and a suitable statistic as a value.
        for i in range(len(self.negation_index_list)):
            map_neg_statistic_comb[self.negation_index_list[i]] = arrivalRates[self.negation_index_list[i]]
        sorted_keys = sorted(map_neg_statistic_comb, key=map_neg_statistic_comb.get)
        tup = tuple(sorted_keys)
        if self.map_comb[tup] == 0:
            self.map_comb[tup] = 1
            self.comb_done = self.comb_done + 1
            if len(self.map_comb) == 1:
                self.last_comb = "(" + str(self.negation_index_list[0]) + ")"
            else:
                self.last_comb = str(tup)
            return True
        else:
            return False

    def print_to_statistics_file(self, result, name, map_to_print=0):
        with open(self.path, 'a') as file:
            file.write(" +++++++++++++++++++++++ summary ++++++++++++++++++++++++ \n")
            file.write("In %s test - %s \n" % (name, result))
            if map_to_print == 1:
                map_status = str(self.map_comb)
                file.write(map_status + "\n")
            file.write(" +++++++++++++++++++++++++++++++++++++++++++++++++++++++ \n")
        file.close()


def runTrees(pattern, expectedFileName, createTestFile, check_all_combs):
    """
    The function runs the test with the three negation algorithms on different statistics. In each loop,
    the negative events get a statistic combination that wasn't checked before, unless the flag "check_all_combs
    is False (in this case only one combination will be tested).
    """
    path = os.path.join(absolutePath, 'test/StatisticsDocumentation/statistics.txt')
    statistics = Statistics(pattern, path)
    combinations_total_num = len(statistics.map_comb)
    max_loops = combinations_total_num * 2
    num_loops_so_far = 0

    while statistics.comb_done < combinations_total_num and num_loops_so_far < max_loops:
        selectivityMatrix, arrivalRates, result = statistics.setStatisticsForTest(expectedFileName)
        num_loops_so_far += 1
        # If the result is False the previous function didn't succeed in finding a statistic combination that wasn't checked before.
        if result is False:
            continue
        print("#### The following tests used the statistic combination " + statistics.last_comb + ":")

        # NAIVE NEGATION ALGORITHM
        runAllTrees(pattern, expectedFileName, createTestFile, selectivityMatrix=selectivityMatrix,
                    arrivalRates=arrivalRates)
        # STATISTIC NEGATION ALGORITHM
        runAllTrees(pattern, expectedFileName, createTestFile, NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM,
                    selectivityMatrix, arrivalRates)
        # LOWEST POSITION NEGATION ALGORITHM
        runAllTrees(pattern, expectedFileName, createTestFile, NegationAlgorithmTypes.LOWEST_POSITION_NEGATION_ALGORITHM
                    , selectivityMatrix, arrivalRates)

        print("### ### ###")

        if check_all_combs is False:
            break
    if combinations_total_num == statistics.comb_done:
        statistics.print_to_statistics_file("all combinations were tested", expectedFileName)
    elif check_all_combs:
        statistics.print_to_statistics_file("missing combinations", expectedFileName, 1)
        numFailedTests.miss_comb.append(expectedFileName)


def runAllTrees(pattern, expectedFileName, createTestFile, negationAlgo=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM
                , selectivityMatrix=None, arrivalRates=None):
    """
    This function runs each test case with all kind of positive trees
    """
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
def multipleNotBeginAndEndTest(createTestFile=False, check_all_combs=False):
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
    runTrees(pattern, "MultipleNotBeginAndEnd", createTestFile, check_all_combs)


# ON custom2
def simpleNotTest(createTestFile=False, check_all_combs=False):
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
    runTrees(pattern, "simpleNot", createTestFile, check_all_combs)


# ON NASDAQ SHORT
def multipleNotInTheMiddleTest(createTestFile=False, check_all_combs=False):
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
    runTrees(pattern, "MultipleNotMiddle", createTestFile, check_all_combs)


# ON NASDAQ SHORT
def oneNotAtTheBeginningTest(createTestFile=False, check_all_combs=False):
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
    runTrees(pattern, "OneNotBegin", createTestFile, check_all_combs)


# ON NASDAQ SHORT
def multipleNotAtTheBeginningTest(createTestFile=False, check_all_combs=False):
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
    runTrees(pattern, "MultipleNotBegin", createTestFile, check_all_combs)


# ON NASDAQ *HALF* SHORT
def oneNotAtTheEndTest(createTestFile=False, check_all_combs=False):
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
    runTrees(pattern, "OneNotEnd", createTestFile, check_all_combs)


# ON NASDAQ *HALF* SHORT
def multipleNotAtTheEndTest(createTestFile=False, check_all_combs=False):
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
    runTrees(pattern, "MultipleNotEnd", createTestFile, check_all_combs)


# ON CUSTOM3
def testWithMultipleNotAtBeginningMiddleEnd(createTestFile=False, check_all_combs=False):
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
    runTrees(pattern, "NotEverywhere", createTestFile, check_all_combs)


# ON CUSTOM
def testWithMultipleNotAtBeginningMiddleEnd2(createTestFile=False, check_all_combs=False):
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
    runTrees(pattern, "NotEverywhere2", createTestFile, check_all_combs)
