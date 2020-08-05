from optimizer.Optimizer import Optimizer
from optimizer.ReoptimizingDecision import ReoptimizationDecisionTypes
import time
from datetime import timedelta
from statisticsCollector.StatisticsTypes import StatisticsTypes
from base.Formula import GreaterThanFormula, SmallerThanFormula, SmallerThanEqFormula, GreaterThanEqFormula, MulTerm, \
    EqFormula, IdentifierTerm, AtomicTerm, AndFormula, TrueFormula
from base.PatternStructure import AndOperator, SeqOperator, QItem
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismTypes


class ReoptimizationParameters:
    def __init__(self, reoptimization_type: ReoptimizationDecisionTypes, data):
        self.type = reoptimization_type
        self.data = data


class Stat2:
    def __init__(self, arrival_rates, selectivity_matrix, statistics_type):
        self.arrival_rates = arrival_rates
        self.selectivity_matrix = selectivity_matrix
        self.statistics_type = statistics_type


def decisions_test(pattern):
    numbered_event_rates_for_thresholds = [[10, 20, 30, 40, 50],
                                           [15, 22, 28, 45, 45],
                                           [26, 22, 28, 40, 36],
                                           [30, 10, 20, 30, 23],
                                           [35, 15, 25, 35, 29]]

    numbered_event_rates_for_invariants = [[10, 20, 30, 40, 50],
                                           [15, 22, 28, 45, 45],
                                           [26, 22, 28, 40, 36],
                                           [30, 10, 20, 30, 23],
                                           [35, 15, 25, 35, 29],
                                           [30, 20, 23, 29, 31],
                                           [10, 20, 23, 29, 31],
                                           [10, 20, 30, 40, 50]]


    selectivity_matrix_for_thresholds = [
        [[1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0],
         [1.0, 1.0, 1.0, 1.0, 1.0]],
        [[1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0],
         [1.0, 1.0, 1.0, 1.0, 1.0]],
        [[1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0],
         [1.0, 1.0, 1.0, 1.0, 1.0]],
        [[1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0],
         [1.0, 1.0, 1.0, 1.0, 1.0]],
        [[1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0],
         [1.0, 1.0, 1.0, 1.0, 1.0]]
    ]

    selectivity_matrix_for_invariants = [
        [[1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0],
         [1.0, 1.0, 1.0, 1.0, 1.0]],
        [[1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0],
         [1.0, 1.0, 1.0, 1.0, 1.0]],
        [[1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0],
         [1.0, 1.0, 1.0, 1.0, 1.0]],
        [[1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0],
         [1.0, 1.0, 1.0, 1.0, 1.0]],
        [[1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0],
         [1.0, 1.0, 1.0, 1.0, 1.0]],
        [[1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0],
         [1.0, 1.0, 1.0, 1.0, 1.0]],
        [[1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0],
         [1.0, 1.0, 1.0, 1.0, 1.0]],
        [[1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0],
         [1.0, 1.0, 1.0, 1.0, 1.0]]
    ]

    threshold_decisions_tests(pattern, EvaluationMechanismTypes.GREEDY_LEFT_DEEP_TREE,
                              numbered_event_rates_for_thresholds, selectivity_matrix_for_thresholds)
    invariants_decision_test(pattern, EvaluationMechanismTypes.GREEDY_LEFT_DEEP_TREE,
                             numbered_event_rates_for_invariants, selectivity_matrix_for_invariants)
    threshold_decisions_tests(pattern, EvaluationMechanismTypes.DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE,
                              numbered_event_rates_for_thresholds, selectivity_matrix_for_thresholds)


def compare_decisions(decisions_array1, decisions_array2):
    if len(decisions_array1) != len(decisions_array2):
        return False
    for decision1, decision2 in zip(decisions_array1, decisions_array2):
        if decision1 != decision2:
            return False
    return True


def compare_invariants(invariants_array1, invariants_array2):
    if len(invariants_array1) != len(invariants_array2):
        print("Something bad happened - Invariants are not in the same size 1")
    for invariant1, invariant2 in zip(invariants_array1, invariants_array2):
        if invariant1[1] is None and invariant2[1] is None:
            if invariant1[0] != invariant2[0]:
                return False
        else:
            if invariant1[0] != invariant2[0] or invariant1[1] != invariant2[1] or len(invariant1[2]) != len(invariant2[2]):
                return False
            for i, j in zip(invariant1[2], invariant2[2]):
                if i != j:
                    return False
    return True


def threshold_decisions_test(pattern, evaluation_mechanism_type, numbered_event_rates, selectivity_matrix, threshold):
    reoptimization_parameters_5 = ReoptimizationParameters(
        ReoptimizationDecisionTypes.STATIC_THRESHOLD_BASED, threshold)
    result_decisions_for_threshold = []
    optimizer = Optimizer(pattern, evaluation_mechanism_type, StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES,
                          reoptimization_parameters_5)
    for i in range(len(numbered_event_rates)):
        stat2 = Stat2(numbered_event_rates[i], selectivity_matrix[i],
                      StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES)
        tree = optimizer.run(stat2)
        if tree is None:
            result_decisions_for_threshold.append(False)
        else:
            result_decisions_for_threshold.append(True)
    return result_decisions_for_threshold


def threshold_decisions_tests(pattern, evaluation_mechanism_type, numbered_event_rates, selectivity_matrix):
    test_result = True
    result_decisions_for_threshold_5 = threshold_decisions_test(pattern, evaluation_mechanism_type,
                                                                numbered_event_rates, selectivity_matrix, 5)
    result_decisions_for_threshold_10 = threshold_decisions_test(pattern, evaluation_mechanism_type,
                                                                 numbered_event_rates, selectivity_matrix, 10)
    result_decisions_for_threshold_15 = threshold_decisions_test(pattern, evaluation_mechanism_type,
                                                                 numbered_event_rates, selectivity_matrix, 15)

    expected_decisions_for_threshold_5 = [True, False, True, True, True]
    expected_decisions_for_threshold_10 = [True, False, True, True, False]
    expected_decisions_for_threshold_15 = [True, False, False, False, False]

    if not compare_decisions(result_decisions_for_threshold_5, expected_decisions_for_threshold_5):
        print("Failed threshold 5")
        test_result = False
    if not compare_decisions(result_decisions_for_threshold_10, expected_decisions_for_threshold_10):
        print("Failed threshold 10")
        test_result = False
    if not compare_decisions(result_decisions_for_threshold_15, expected_decisions_for_threshold_15):
        print("Failed threshold 15")
        test_result = False

    if test_result:
        print("Passed Threshold tests")



def invariants_decision_test(pattern, evaluation_mechanism_type, numbered_event_rates, selectivity_matrix):
    # Assuming that the selectivity matrix will be full of 1's
    test_result = True
    expected_invariants_at_each_new_data = [
        [(0, 1, []), (1, 2, [0]), (2, 3, [0, 1]), (3, 4, [0, 1, 2]), (4, None, None)],
        [(0, 1, []), (1, 2, [0]), (2, 3, [0, 1]), (3, 4, [0, 1, 2]), (4, None, None)],  # same as before
        [(1, 0, []), (0, 2, [1]), (2, 4, [1, 0]), (4, 3, [1, 0, 2]), (3, None, None)],
        [(1, 2, []), (2, 4, [1]), (4, 0, [1, 2]), (3, 0, [1, 2, 4]), (0, None, None)],  # The (3, 0, [1, 2, 4]) is becasue of the way the oredring algorithm is (popping and returning variables)
        [(1, 2, []), (2, 4, [1]), (4, 0, [1, 2]), (3, 0, [1, 2, 4]), (0, None, None)],  # same as before
        [(1, 2, []), (2, 3, [1]), (3, 0, [1, 2]), (0, 4, [1, 2, 3]), (4, None, None)],
        [(0, 1, []), (1, 2, [0]), (2, 3, [0, 1]), (3, 4, [0, 1, 2]), (4, None, None)],
        [(0, 1, []), (1, 2, [0]), (2, 3, [0, 1]), (3, 4, [0, 1, 2]), (4, None, None)]]  # same as before
    expected_decisions_for_invariants = [True, False, True, True, False, True, True, False]
    reoptimization_parameters = ReoptimizationParameters(
        ReoptimizationDecisionTypes.INVARIANT_BASED, None)
    invariants_array = []
    result_decisions_for_invariants = []
    optimizer = Optimizer(pattern, evaluation_mechanism_type, StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES,
                          reoptimization_parameters)
    for i in range(len(numbered_event_rates)):
        stat2 = Stat2(numbered_event_rates[i], selectivity_matrix[i],
                      StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES)
        tree, invariants = optimizer.testing_invariants(stat2)
        if tree is None:
            result_decisions_for_invariants.append(False)
            invariants_array.append(invariants_array[i - 1])
        else:
            result_decisions_for_invariants.append(True)
            invariants_array.append(invariants)
    if not compare_decisions(result_decisions_for_invariants, expected_decisions_for_invariants):
        print("Failed Invariants decisions")
        test_result = False
    for invariants_result, invariants_expected in zip(invariants_array, expected_invariants_at_each_new_data):
        if not compare_invariants(invariants_result, invariants_expected):
            print("Failed Invariants themselves")
            test_result = False
            break
    if test_result:
        print("Passed Invariants test")
        return True
    else:
        return False


def main():
    """
        PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
        WHERE   a.OpeningPrice > b.OpeningPrice
            AND b.OpeningPrice > c.OpeningPrice
        WITHIN 5 minutes
        """
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c"), QItem("GOOG", "d"),
                     QItem("JASO", "e")]),
        AndFormula(
            AndFormula(
                AndFormula(
                GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                                   IdentifierTerm("b", lambda x: x["Opening Price"])),
                GreaterThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                                   IdentifierTerm("c", lambda x: x["Opening Price"]))),
                    GreaterThanFormula(IdentifierTerm("c", lambda x: x["Opening Price"]),
                                    IdentifierTerm("d", lambda x: x["Opening Price"]))),
                        GreaterThanFormula(IdentifierTerm("d", lambda x: x["Opening Price"]),
                                        IdentifierTerm("e", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    decisions_test(pattern)


if __name__ == "__main__":
    main()
