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
import json

class Statistics:
    def __init__(self, pattern, path):
        self.path = path
        self.negation_index_list = []
        self.map_comb = {}
        self.comb_done = 0
        self.last_comb = ""
        self.eventsNum = len(pattern.full_structure.get_args())
        for i, arg in enumerate(pattern.full_structure.get_args()):
            if type(arg) == NegationOperator:
                # a negative event was found and needs to be extracted
                self.negation_index_list.append(i)
        for val in list(permutations(self.negation_index_list)):
            self.map_comb[val] = 0

    def setStatisticsForTest(self, expectedFileName):
        eventsNum = self.eventsNum
        arrivalRates = [*random.rand(eventsNum)]
        counter = 0
        while (self.is_good_statistics(arrivalRates) is False and (counter < 5)):
            arrivalRates = [*random.rand(eventsNum)]
            counter = counter + 1
        selectivityMatrixA = random.rand(eventsNum, eventsNum)
        selectivityMatrix = selectivityMatrixA.T * selectivityMatrixA
        fill_diagonal(selectivityMatrix, 1)
        selectivityMatrix = selectivityMatrix.tolist()
        with open(self.path, 'a') as file:
            file.write("\nStatistics values in %s tests: \n\n" % (expectedFileName))
            file.write("negation combination: %s \n" % (self.last_comb))
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

    def is_good_statistics(self, arrivalRates):
        map_neg_statistic_comb = {}
        for i in range(len(self.negation_index_list)):
            map_neg_statistic_comb[self.negation_index_list[i]] = arrivalRates[self.negation_index_list[i]]
        sorted_keys = sorted(map_neg_statistic_comb, key=map_neg_statistic_comb.get)
        tup = tuple(sorted_keys)
        if self.map_comb[tup] == 0:
            self.map_comb[tup] = 1
            if len(self.map_comb) == 1:
                self.last_comb = "(" + str(self.negation_index_list[0]) + ")"
            else:
                self.last_comb = str(tup)
            self.comb_done = self.comb_done + 1
            return True
        else:
            return False

    def print_to_statistics_file(self,result, name, to_print_map =0):
        with open(self.path, 'a') as file:
            file.write(" +++++++++++++++++++++++ summary ++++++++++++++++++++++++ \n")
            file.write("In %s test - %s \n" % (name, result))
            if to_print_map ==1:
                map_status = str(self.map_comb)
                file.write(map_status+ "\n")
            file.write(" +++++++++++++++++++++++++++++++++++++++++++++++++++++++ \n")
        file.close()

def runTrees(pattern, expectedFileName, createTestFile):
    path = os.path.join(absolutePath, 'test/StatisticsDocumentation/statistics.txt')
    statistics = Statistics(pattern , path)
    combinations_total_num = len(statistics.map_comb)
    max_loops= combinations_total_num*2
    #max_loops = 2 # missing combinations example

    while(statistics.comb_done < combinations_total_num and statistics.comb_done < max_loops):
        selectivityMatrix, arrivalRates = statistics.setStatisticsForTest(expectedFileName)
        print("#### The following tests used the statistic combination " + statistics.last_comb +":")
        # NAIVE NEGATION ALGORITHM
        runAllTrees(pattern, expectedFileName, createTestFile, selectivityMatrix=selectivityMatrix,
                    arrivalRates=arrivalRates )
        # STATISTIC NEGATION ALGORITHM
        runAllTrees(pattern, expectedFileName, createTestFile, NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM,
                    selectivityMatrix,arrivalRates )
        print("### ### ###")
    if combinations_total_num == statistics.comb_done:
        statistics.print_to_statistics_file("all combinations were tested", expectedFileName)
    else:
        statistics.print_to_statistics_file("missing combinations", expectedFileName,1)

def runAllTrees(pattern, expectedFileName, createTestFile, negationAlgo=NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM
                ,selectivityMatrix = None, arrivalRates = None):


    # Trivial tree
    origPatt = copy.deepcopy(pattern)
    origPatt.set_statistics(StatisticsTypes.ARRIVAL_RATES, arrivalRates)
    runTest(expectedFileName, [origPatt], createTestFile, negation_algorithm=negationAlgo, testName="Trivial ")

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
    runTrees(pattern, "MultipleNotBeginAndEnd", createTestFile)

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
    runTrees(pattern, "simpleNot", createTestFile)


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
    runTrees(pattern, "MultipleNotMiddle", createTestFile)


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
    runTrees(pattern, "OneNotBegin", createTestFile)


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
    runTrees(pattern, "MultipleNotBegin", createTestFile)


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
    runTrees(pattern, "OneNotEnd", createTestFile)


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
    runTrees(pattern, "MultipleNotEnd", createTestFile)

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
    runTrees(pattern, "NotEverywhere", createTestFile)
