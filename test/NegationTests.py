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
from negationAlgorithms.NegationAlgorithm import NegationAlgorithmTypes
from misc import DefaultConfig


class Statistics:
    def __init__(self, pattern, path):
        self.path = path
        self.negation_index_list = []
        self.map_combination = {} # key: negative events' indices combination, value : 0  or 1 (if the combination was checked).
        self.combination_done = 0
        self.last_combination = ""
        self.events_num = len(pattern.full_structure.args)
        # Collecting the negative events' indices
        for i, arg in enumerate(pattern.full_structure.args):
            if type(arg) == NegationOperator:
                self.negation_index_list.append(i)
        # Generating all the possible combinations for the negative events.
        for value in list(permutations(self.negation_index_list)):
            self.map_combination[value] = 0

    def set_statistics_for_test(self, expected_file_name):
        """
        The function searches for statistic with negative indexes combination that wasn't checked before.
        If it succeeds, it returns the statistics, otherwise it returns None.
        """
        events_num = self.events_num
        arrival_rates = [*random.rand(events_num)]
        counter = 0
        # Maximum searching loops for a new combination
        max_loops = 5
        is_valid = self.is_statistic_valid(arrival_rates)
        while counter < max_loops and is_valid is False:
            arrival_rates = [*random.rand(events_num)]
            counter = counter + 1
            is_valid = self.is_statistic_valid(arrival_rates)
        if counter == max_loops and is_valid is False:
            return None, None, False
        selectivity_matrix_a = random.rand(events_num, events_num)
        selectivity_matrix = selectivity_matrix_a.T * selectivity_matrix_a
        fill_diagonal(selectivity_matrix, 1)
        selectivity_matrix = selectivity_matrix.tolist()

        # Documentation of the new statistics.
        with open(self.path, 'a') as file:
            file.write("\nStatistics values in %s tests: \n\n" % expected_file_name)
            file.write("negation combination: %s \n" % self.last_combination)
            file.write("arrivalRates: [ ")
            for i in arrival_rates:
                file.write(str(i) + " ")
            file.write("]" + "\n")
            file.write("selectivityMatrix:" + "\n")
            for line in selectivity_matrix:
                file.write("[")
                for i in line:
                    file.write(str(i) + " ")
                file.write("]" + "\n")
            file.write("\n**********************\n")
        file.close()
        return selectivity_matrix, arrival_rates, True

    def is_statistic_valid(self, arrival_rates):
        """
        The function returns True if a statistic with the same negative index combination wasn't checked before.
        """
        map_negation_statistic_combination = {}
        # Creating a map with a negative event index as a key and a suitable statistic as a value.
        for i in range(len(self.negation_index_list)):
            map_negation_statistic_combination[self.negation_index_list[i]] = arrival_rates[self.negation_index_list[i]]
        sorted_keys = sorted(map_negation_statistic_combination, key=map_negation_statistic_combination.get)
        last_combination_tuple = tuple(sorted_keys)
        if self.map_combination[last_combination_tuple] == 0:
            self.map_combination[last_combination_tuple] = 1
            self.combination_done = self.combination_done + 1
            if len(self.map_combination) == 1:
                self.last_combination = "(" + str(self.negation_index_list[0]) + ")"
            else:
                self.last_combination = str(last_combination_tuple)
            return True
        else:
            return False

    def print_to_statistics_file(self, result, name, map_to_print=0):
        """
        Prints the statistics combinations that were chosen.
        """
        with open(self.path, 'a') as file:
            file.write(" +++++++++++++++++++++++ summary ++++++++++++++++++++++++ \n")
            file.write("In %s test - %s \n" % (name, result))
            if map_to_print == 1:
                map_status = str(self.map_combination)
                file.write(map_status + "\n")
            file.write(" +++++++++++++++++++++++++++++++++++++++++++++++++++++++ \n")
        file.close()


def run_trees(pattern, expected_file_name, create_test_file, check_all_combinations):
    """
    The function runs the test with the three negation algorithms on different statistics. In each loop,
    the negative events get a statistic combination that wasn't checked before, unless the flag "check_all_combs
    is False (in this case only one combination will be tested).
    """
    path = os.path.join(absolutePath, 'test/StatisticsDocumentation/statistics.txt')
    statistics = Statistics(pattern, path)
    combinations_total_num = len(statistics.map_combination)
    max_loops = combinations_total_num * 2
    num_loops_so_far = 0

    while statistics.combination_done < combinations_total_num and num_loops_so_far < max_loops:
        selectivity_matrix, arrival_rates, result = statistics.set_statistics_for_test(expected_file_name)
        num_loops_so_far += 1
        # If the result is False the previous function didn't succeed in finding a statistic combination that wasn't checked before.
        if result is False:
            continue
        print("#### The following tests used the statistic combination " + statistics.last_combination + ":")
        # NAIVE NEGATION ALGORITHM
        runAllTrees(pattern, expected_file_name, create_test_file, selectivity_matrix=selectivity_matrix,
                    arrival_rates=arrival_rates)
        # STATISTIC NEGATION ALGORITHM
        runAllTrees(pattern, expected_file_name, create_test_file, NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM,
                    selectivity_matrix, arrival_rates)
        # LOWEST POSITION NEGATION ALGORITHM
        runAllTrees(pattern, expected_file_name, create_test_file, NegationAlgorithmTypes.LOWEST_POSITION_NEGATION_ALGORITHM
                    , selectivity_matrix, arrival_rates)

        print("### ### ###")

        if check_all_combinations is False:
            break
    if combinations_total_num == statistics.combination_done:
        statistics.print_to_statistics_file("all combinations were tested", expected_file_name)
    elif check_all_combinations:
        statistics.print_to_statistics_file("missing combinations", expected_file_name, 1)
        num_failed_tests.missing_combination.append(expected_file_name)


def runAllTrees(pattern, expected_file_name, create_test_file, negationAlgorithm=DefaultConfig.
                DEFAULT_NEGATION_ALGORITHM, selectivity_matrix=None, arrival_rates=None):
    """
    This function runs each test case with all kind of positive trees
    """
    # Trivial tree
    original_pattern = copy.deepcopy(pattern)
    original_pattern.set_statistics(StatisticsTypes.ARRIVAL_RATES, arrival_rates)
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params, negation_algorithm_type=negationAlgorithm
    )
    runTest(expected_file_name, [original_pattern], create_test_file, eval_params, test_name="Trivial")

    # SORT_BY_FREQUENCY_LEFT_DEEP_TREE
    original_pattern = copy.deepcopy(pattern)
    original_pattern.set_statistics(StatisticsTypes.ARRIVAL_RATES, arrival_rates)
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params, negation_algorithm_type=negationAlgorithm
    )
    runTest(expected_file_name, [original_pattern], create_test_file, eval_params, test_name="Sort")

    # GREEDY_LEFT_DEEP_TREE
    original_pattern = copy.deepcopy(pattern)
    original_pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivity_matrix, arrival_rates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.GREEDY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params, negation_algorithm_type=negationAlgorithm
    )
    runTest(expected_file_name, [original_pattern], create_test_file, eval_params, nasdaqEventStream, test_name="Greedy")

    # LOCAL_SEARCH_LEFT_DEEP_TREE
    # this tree was not checked yet (at all)

    # DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE
    original_pattern = copy.deepcopy(pattern)
    original_pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivity_matrix, arrival_rates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params, negation_algorithm_type=negationAlgorithm
    )
    runTest(expected_file_name, [original_pattern], create_test_file, eval_params, nasdaqEventStream, test_name="DP")

    # DYNAMIC_PROGRAMMING_BUSHY_TREE
    original_pattern = copy.deepcopy(pattern)
    original_pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivity_matrix, arrival_rates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params, negation_algorithm_type=negationAlgorithm
    )
    runTest(expected_file_name, [original_pattern], create_test_file, eval_params, nasdaqEventStream, test_name="DPBushy")

    # ZSTREAM_BUSHY_TREE
    original_pattern = copy.deepcopy(pattern)
    original_pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivity_matrix, arrival_rates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.ORDERED_ZSTREAM_BUSHY_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params, negation_algorithm_type=negationAlgorithm
    )
    runTest(expected_file_name, [original_pattern], create_test_file, eval_params, nasdaqEventStream, test_name="ZSBushy")

    # ORDERED_ZSTREAM_BUSHY_TREE
    original_pattern = copy.deepcopy(pattern)
    original_pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivity_matrix, arrival_rates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.ORDERED_ZSTREAM_BUSHY_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params, negation_algorithm_type=negationAlgorithm
    )
    runTest(expected_file_name, [original_pattern], create_test_file, eval_params, nasdaqEventStream, test_name="OrderedZSBushy")


# ON CUSTOM
def multipleNotBeginAndEndTest(create_test_file=False, check_all_combinations=True):
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
    run_trees(pattern, "MultipleNotBeginAndEnd", create_test_file, check_all_combinations)


# ON custom2
def simpleNotTest(create_test_file=False, check_all_combinations=True):
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
    run_trees(pattern, "simpleNot", create_test_file, check_all_combinations)


# ON NASDAQ SHORT
def multipleNotInTheMiddleTest(create_test_file=False, check_all_combinations=True):
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
    run_trees(pattern, "MultipleNotMiddle", create_test_file, check_all_combinations)


# ON NASDAQ SHORT
def oneNotAtTheBeginningTest(create_test_file=False, check_all_combinations=True):
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
    run_trees(pattern, "OneNotBegin", create_test_file, check_all_combinations)


# ON NASDAQ SHORT
def multipleNotAtTheBeginningTest(create_test_file=False, check_all_combinations=True):
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
    run_trees(pattern, "MultipleNotBegin", create_test_file, check_all_combinations)


# ON NASDAQ *HALF* SHORT
def oneNotAtTheEndTest(create_test_file=False, check_all_combinations=True):
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
    run_trees(pattern, "OneNotEnd", create_test_file, check_all_combinations)


# ON NASDAQ *HALF* SHORT
def multipleNotAtTheEndTest(create_test_file=False, check_all_combinations=True):
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
    run_trees(pattern, "MultipleNotEnd", create_test_file, check_all_combinations)


# ON CUSTOM3
def testWithMultipleNotAtBeginningMiddleEnd(create_test_file=False, check_all_combinations=True):
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
    run_trees(pattern, "NotEverywhere", create_test_file, check_all_combinations)


# ON CUSTOM
def testWithMultipleNotAtBeginningMiddleEnd2(create_test_file=False, check_all_combinations=True):
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
    run_trees(pattern, "NotEverywhere2", create_test_file, check_all_combinations)
