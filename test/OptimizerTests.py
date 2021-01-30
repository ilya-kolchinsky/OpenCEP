from datetime import timedelta

from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, PrimitiveEventStructure
from condition.Condition import SimpleCondition, Variable
from misc.OptimizerTypes import OptimizerTypes
from misc.StatisticsTypes import StatisticsTypes
from optimizer.Optimizer import Optimizer
from optimizer.OptimizerFactory import OptimizerFactory, InvariantsAwareOptimizerParameters

from plan.TreeCostModels import TreeCostModels
from plan.TreePlanBuilderFactory import TreePlanBuilderParameters, SelectivityAndArrivalRatesWrapper, StatisticsWrapper
from plan.TreePlanBuilderTypes import TreePlanBuilderTypes
from test.NewTestsUtils import DEFAULT_TESTING_GREEDY_INVARIANT_OPTIMIZER_SETTINGS, \
    DEFAULT_TESTING_ZSTREAM_INVARIANT_OPTIMIZER_SETTINGS, DEFAULT_TESTING_TRIVIAL_OPTIMIZER_SETTINGS_WITH_ZSTREAM


def get_pattern_test():
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("LOCM", "c")),
        SimpleCondition(Variable("a", lambda x: x["Opening Price"]),
                        Variable("b", lambda x: x["Opening Price"]),
                        Variable("c", lambda x: x["Opening Price"]),
                        relation_op=lambda x, y, z: x > y > z),
        timedelta(minutes=5)
    )
    return pattern


def greedy_invariant_optimizer_doesnt_change_the_tree_1():
    """
    Basic test, check if greedy invariant aware optimizer say that
    need to generate new tree in the case that statistics doesnt change
    Expected:
    The optimizer will say that there is no need to re-optimize
    """
    optimizer_parameters = DEFAULT_TESTING_GREEDY_INVARIANT_OPTIMIZER_SETTINGS
    optimizer = OptimizerFactory.build_optimizer(optimizer_parameters)

    old_arrival_rates = [1, 5, 8, 2]
    old_selectivity_matrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                          [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    old_statistics = SelectivityAndArrivalRatesWrapper(old_arrival_rates, old_selectivity_matrix)

    new_arrival_rates = [1, 5, 8, 2]
    new_selectivity_matrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                          [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    new_statistics = SelectivityAndArrivalRatesWrapper(new_arrival_rates, new_selectivity_matrix)

    is_changed = optimizer_behavior_test_function(old_statistics, new_statistics, optimizer)
    if not is_changed:
        print("Success")
    else:
        print("Failed")


def greedy_invariant_optimizer_doesnt_change_the_tree_2():
    """
    In this test:
    We change the statistics so that the conditions in invariants should not change yet.
    Expected:
    The optimizer will say that there is no need to re-optimize
    """
    optimizer_parameters = DEFAULT_TESTING_GREEDY_INVARIANT_OPTIMIZER_SETTINGS
    optimizer = OptimizerFactory.build_optimizer(optimizer_parameters)

    old_arrival_rates = [1, 5, 8, 2]
    old_selectivity_matrix = [[1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0],
                          [1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0]]
    old_statistics = SelectivityAndArrivalRatesWrapper(old_arrival_rates, old_selectivity_matrix)

    new_arrival_rates = [1, 7, 8, 2]
    new_selectivity_matrix = [[1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0],
                          [1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0]]
    new_statistics = SelectivityAndArrivalRatesWrapper(new_arrival_rates, new_selectivity_matrix)

    is_changed = optimizer_behavior_test_function(old_statistics, new_statistics, optimizer)
    if not is_changed:
        print("Success")
    else:
        print("Failed")


def greedy_invariant_optimizer_change_the_tree_1():
    """
    In this test:
    We change the statistics so that the conditions in invariants should be change.
    Expected:
    The optimizer will say that need to re-optimize
    """
    optimizer_parameters = DEFAULT_TESTING_GREEDY_INVARIANT_OPTIMIZER_SETTINGS
    optimizer = OptimizerFactory.build_optimizer(optimizer_parameters)

    old_arrival_rates = [1, 5, 8, 2]
    old_selectivity_matrix = [[1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0],
                          [1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0]]
    old_statistics = SelectivityAndArrivalRatesWrapper(old_arrival_rates, old_selectivity_matrix)

    new_arrival_rates = [2, 5, 8, 1]
    new_selectivity_matrix = [[1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0],
                          [1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0]]
    new_statistics = SelectivityAndArrivalRatesWrapper(new_arrival_rates, new_selectivity_matrix)

    is_changed = optimizer_behavior_test_function(old_statistics, new_statistics, optimizer)
    if not is_changed:
        print("Failed")
    else:
        print("Success")


def zstream_invariant_optimizer_doesnt_change_the_tree_1():
    """
    Basic test, check if zstream invariant aware optimizer say that
    need to generate new tree in the case that statistics doesnt change
    Expected:
    The optimizer will say that there is no need to re-optimize
    """
    optimizer_parameters = DEFAULT_TESTING_ZSTREAM_INVARIANT_OPTIMIZER_SETTINGS
    optimizer = OptimizerFactory.build_optimizer(optimizer_parameters)

    old_arrival_rates = [5, 1, 8, 2]
    old_selectivity_matrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                      [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    old_statistics = SelectivityAndArrivalRatesWrapper(old_arrival_rates, old_selectivity_matrix)

    new_arrival_rates = [1, 5, 8, 2]
    new_selectivity_matrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                      [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    new_statistics = SelectivityAndArrivalRatesWrapper(new_arrival_rates, new_selectivity_matrix)

    is_changed = optimizer_behavior_test_function(old_statistics, new_statistics, optimizer)
    if not is_changed:
        print("Success")
    else:
        print("Failed")


def zstream_invariant_optimizer_change_the_tree_1():
    """
    Basic test, check if zstream invariant aware optimizer say that
    need to generate new tree in the case that statistics doesnt change
    Expected:
    The optimizer will say that need to re-optimize
    """
    optimizer_parameters = DEFAULT_TESTING_ZSTREAM_INVARIANT_OPTIMIZER_SETTINGS
    optimizer = OptimizerFactory.build_optimizer(optimizer_parameters)

    old_arrival_rates = [8, 5, 1, 2, 3]
    old_selectivity_matrix = [[1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0],
                      [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0]]
    old_statistics = SelectivityAndArrivalRatesWrapper(old_arrival_rates, old_selectivity_matrix)

    new_arrival_rates = [1, 5, 3, 2, 1]
    new_selectivity_matrix = [[1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0],
                      [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0]]
    new_statistics = SelectivityAndArrivalRatesWrapper(new_arrival_rates, new_selectivity_matrix)

    is_changed = optimizer_behavior_test_function(old_statistics, new_statistics, optimizer)
    if is_changed:
        print("Success")
    else:
        print("Failed")


def zstream_invariant_optimizer_change_the_tree_2():
    """
    Basic test, check if zstream invariant aware optimizer say that
    need to generate new tree in the case that statistics doesnt change
    Expected:
    The optimizer will say that need to re-optimize
    """
    optimizer_parameters = DEFAULT_TESTING_ZSTREAM_INVARIANT_OPTIMIZER_SETTINGS
    optimizer = OptimizerFactory.build_optimizer(optimizer_parameters)

    old_arrival_rates = [8, 5, 1, 2, 3]
    old_selectivity_matrix = [[1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0],
                      [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0]]
    old_statistics = SelectivityAndArrivalRatesWrapper(old_arrival_rates, old_selectivity_matrix)

    new_arrival_rates = [8, 5, 9, 2, 3]
    new_selectivity_matrix = [[1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0],
                      [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0]]
    new_statistics = SelectivityAndArrivalRatesWrapper(new_arrival_rates, new_selectivity_matrix)

    is_changed = optimizer_behavior_test_function(old_statistics, new_statistics, optimizer)
    if is_changed:
        print("Success")
    else:
        print("Failed")


def optimizer_behavior_test_function(old_statistics: StatisticsWrapper, new_statistics: StatisticsWrapper, optimizer: Optimizer):

    pattern = get_pattern_test()

    if optimizer.is_need_optimize(old_statistics, pattern):
        tree_plan = optimizer.build_new_tree_plan(old_statistics, pattern)

    if optimizer.is_need_optimize(new_statistics, pattern):
        tree_plan = optimizer.build_new_tree_plan(new_statistics, pattern)
        return True

    return False
