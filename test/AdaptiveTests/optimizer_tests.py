from optimizer.Optimizer import Optimizer
from optimizer.ReoptimizingDecision import ReoptimizationDecisionTypes
from datetime import timedelta
from statisticsCollector.StatisticsTypes import StatisticsTypes
from base.Formula import GreaterThanFormula, IdentifierTerm, AndFormula
from base.PatternStructure import SeqOperator, QItem
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismTypes


class ReoptimizingDecisionParameters():
    def __init__(self, type: ReoptimizationDecisionTypes):
        self.type = type


class UnconditionalPeriodicalParameters(ReoptimizingDecisionParameters):
    def __init__(self, type: ReoptimizationDecisionTypes, time_limit_data: float):
        super().__init__(type)
        self.data = time_limit_data


class RelativeThresholdlParameters(ReoptimizingDecisionParameters):
    def __init__(self, type: ReoptimizationDecisionTypes, threshold_data: float):
        super().__init__(type)
        self.data = threshold_data


class InvariantsParameters(ReoptimizingDecisionParameters):
    def __init__(self, type: ReoptimizationDecisionTypes, invariant_data):
        super().__init__(type)
        self.data = None


class Stat2:
    def __init__(self, arrival_rates, selectivity_matrix, statistics_type):
        self.arrival_rates = arrival_rates
        self.selectivity_matrix = selectivity_matrix
        self.statistics_type = statistics_type

    def get_generic_data(self):
        generic_data = []
        for rate in self.arrival_rates:
            generic_data.insert(0, rate)
        return generic_data


class AdaptiveParams:
    def __init__(self, statistics_type: StatisticsTypes, reoptimizing_decision_params: ReoptimizingDecisionParameters):
        self.statistics_type = statistics_type
        self.reoptimizing_decision_params = reoptimizing_decision_params


class EvalMechanismParams:
    def __init__(self, eval_mechanism_type: EvaluationMechanismTypes, adaptive_parameters: AdaptiveParams = None):
        self.type = eval_mechanism_type
        self.adaptive_parameters = adaptive_parameters


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
    reoptimization_parameters = UnconditionalPeriodicalParameters(
        ReoptimizationDecisionTypes.RELATIVE_THRESHOLD_BASED, threshold)
    adaptive_params = AdaptiveParams(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, reoptimization_parameters)
    eval_mechanism_params = EvalMechanismParams(EvaluationMechanismTypes.GREEDY_LEFT_DEEP_TREE, adaptive_params)
    result_decisions_for_threshold = []
    optimizer = Optimizer(pattern, evaluation_mechanism_type, eval_mechanism_params)
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
    result_decisions_for_threshold_50 = threshold_decisions_test(pattern, evaluation_mechanism_type,
                                                                numbered_event_rates, selectivity_matrix, 50)
    result_decisions_for_threshold_30 = threshold_decisions_test(pattern, evaluation_mechanism_type,
                                                                 numbered_event_rates, selectivity_matrix, 30)
    result_decisions_for_threshold_65 = threshold_decisions_test(pattern, evaluation_mechanism_type,
                                                                 numbered_event_rates, selectivity_matrix, 65)

    expected_decisions_for_threshold_50 = [True, False, True, True, False]
    expected_decisions_for_threshold_30 = [True, True, True, True, True]
    expected_decisions_for_threshold_65 = [True, False, True, False, False]

    if not compare_decisions(result_decisions_for_threshold_50, expected_decisions_for_threshold_50):
        print("Failed threshold 50%")
        test_result = False
    if not compare_decisions(result_decisions_for_threshold_30, expected_decisions_for_threshold_30):
        print("Failed threshold 30%")
        test_result = False
    if not compare_decisions(result_decisions_for_threshold_65, expected_decisions_for_threshold_65):
        print("Failed threshold 65")
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
    reoptimization_parameters = InvariantsParameters(
        ReoptimizationDecisionTypes.INVARIANT_BASED, None)
    adaptive_params = AdaptiveParams(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, reoptimization_parameters)
    eval_mechanism_params = EvalMechanismParams(EvaluationMechanismTypes.GREEDY_LEFT_DEEP_TREE, adaptive_params)
    invariants_array = []
    result_decisions_for_invariants = []
    optimizer = Optimizer(pattern, evaluation_mechanism_type, eval_mechanism_params)
    for i in range(len(numbered_event_rates)):
        stat2 = Stat2(numbered_event_rates[i], selectivity_matrix[i],
                      StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES)
        plan = optimizer.run(stat2)
        if plan is None:
            result_decisions_for_invariants.append(False)
            invariants_array.append(invariants_array[i - 1])
        else:
            invariants = optimizer.reoptimizing_decision.invariants
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
