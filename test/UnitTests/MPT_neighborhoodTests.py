import random
import string
from datetime import timedelta

from SimulatedAnnealing import tree_plan_visualize_annealing
from base.PatternStructure import SeqOperator, PrimitiveEventStructure, NegationOperator, AndOperator
from condition.BaseRelationCondition import GreaterThanCondition, SmallerThanCondition, SmallerThanEqCondition, \
    GreaterThanEqCondition
from condition.CompositeCondition import AndCondition
from condition.Condition import Variable
from misc.StatisticsTypes import StatisticsTypes
from test.testUtils import *
from plan.MPT_neighborhood import algoA, patterns_initialize_function, tree_plan_state_get_summary, tree_plan_equal, \
    tree_plan_cost_function, tree_plan_neighbour


# -------------------------------------------- Tests for MPT_neighborhood --------------------------------------------

def shareable_all_pairs_unit_test():
    events = ["AAPL", "AMZN", "GOOG"]
    patterns = []
    for i in range(0, 3):
        names = [''.join(random.choice(string.ascii_lowercase) for i in range(5)) for i in range(3)]
        random.shuffle(events)
        patterns.append(Pattern(
            SeqOperator(PrimitiveEventStructure(events[0], names[0]), PrimitiveEventStructure(events[0], names[1]),
                        PrimitiveEventStructure(events[2], names[2])),
            AndCondition(
                GreaterThanCondition(Variable(names[0], lambda x: x["Peak Price"]), 135),
                GreaterThanCondition(Variable(names[0], lambda x: x["Opening Price"]),
                                     Variable(names[1], lambda x: x["Opening Price"])),
                GreaterThanCondition(Variable(names[1], lambda x: x["Opening Price"]),
                                     Variable(names[2], lambda x: x["Opening Price"]))),
            timedelta(minutes=5)
        ))
    eval_mechanism_params = TreeBasedEvaluationMechanismParameters()
    tree_plan_builder = TreePlanBuilderFactory.create_tree_plan_builder(eval_mechanism_params.tree_plan_params)
    tree_plan = tree_plan_builder.build_tree_plan(patterns[0])

    pattern_to_tree_plan_map = {p: tree_plan_builder.build_tree_plan(p) for p in patterns}

    shareable_pairs = algoA.get_all_sharable_sub_patterns(pattern_to_tree_plan_map[patterns[0]], patterns[0],
                                                          pattern_to_tree_plan_map[patterns[1]], patterns[0])
    print('Ok')
    return pattern_to_tree_plan_map


def create_topology_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("GOOG", "d")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern1.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    algoA_instance = algoA()
    pattern_to_tree_plan = algoA_instance._create_topology_with_const_sub_order(pattern1, [0, 3])
    print('Ok')
    return pattern_to_tree_plan_map

def create_topology_const_sub_pattern_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("GOO", "d")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern1.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))

    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895]
    pattern2.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    algoA_instance = algoA()
    type = list(pattern2.get_all_event_types())

    # indexes = [index for index in range(len(pattern2.get_all_event_types()))]
    # names = [pattern2.full_structure.args[i].name for i in indexes]
    tuple1 = (pattern2, range(2), {'a', 'b'})
    pattern_to_tree_plan_map = algoA_instance._create_tree_topology_shared_subpattern(pattern1, tuple1)
    print('Ok')
    return pattern_to_tree_plan_map


def create_topology_sub_pattern_eq_pattern_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("GOOG", "d")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("GOOG", "d")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern1.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern2.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))

    patterns = [pattern1, pattern2]
    eval_mechanism_params = TreeBasedEvaluationMechanismParameters()
    tree_plan_builder = TreePlanBuilderFactory.create_tree_plan_builder(eval_mechanism_params.tree_plan_params)
    tree_plan = tree_plan_builder.build_tree_plan(pattern1)
    pattern_to_tree_plan_map = {p: tree_plan_builder.build_tree_plan(p) for p in patterns}

    algoA_instance = algoA()
    pattern2_data = (pattern2, range(4), {'a', 'b', 'c', 'd'})
    pattern_to_tree_plan_map = algoA_instance._create_tree_topology_shared_subpattern(pattern1, pattern2_data)
    print('Ok')
    return pattern_to_tree_plan_map


def Nedge_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("GOOG", "d")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("GOOG", "d")),
        AndCondition(),
        timedelta(minutes=5)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern1.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))

    selectivityMatrix = [[1.0, 0.15989723367389616], [1.0, 1.0]]
    arrivalRates = [0.013917884481558803, 0.012421711899791231]
    pattern2.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    patterns = [pattern1, pattern2]

    state = patterns_initialize_function(patterns)
    pattern_to_tree_plan_map, shareable_pairs = state
    alg = algoA()
    alg.Nedge_neighborhood(pattern_to_tree_plan_map, shareable_pairs)
    print('Ok')
    return pattern_to_tree_plan_map


def annealing_basic_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("GOO", "d")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("GOO", "d")),
        AndCondition(),
        timedelta(minutes=5)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern1.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))

    selectivityMatrix = [[1.0, 0.15989723367389616], [1.0, 1.0]]
    arrivalRates = [0.013917884481558803, 0.012421711899791231]
    pattern2.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    patterns = [pattern1, pattern2]
    state, c = tree_plan_visualize_annealing(patterns=patterns,
                                             initialize_function=patterns_initialize_function,
                                             state_repr_function=tree_plan_state_get_summary,
                                             state_equal_function=tree_plan_equal,
                                             cost_function=tree_plan_cost_function,
                                             neighbour_function=tree_plan_neighbour)
    pattern_to_tree_plan_map, shareable_pairs = state
    return pattern_to_tree_plan_map


def annealing_med_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("SALEH", "d")),
        AndCondition(
            GreaterThanCondition(Variable("b", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "e"), PrimitiveEventStructure("SALEH", "f")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    pattern3 = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("b", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern1.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern2.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))

    selectivityMatrix = [[1.0, 0.9457796098355941], [0.9457796098355941, 0.15989723367389616]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895]

    pattern3.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    patterns = [pattern1, pattern2, pattern3]
    state, c = tree_plan_visualize_annealing(patterns=patterns,
                                             initialize_function=patterns_initialize_function,
                                             state_repr_function=tree_plan_state_get_summary,
                                             state_equal_function=tree_plan_equal,
                                             cost_function=tree_plan_cost_function,
                                             neighbour_function=tree_plan_neighbour)
    pattern_to_tree_plan_map, shareable_pairs = state
    return pattern_to_tree_plan_map


def Nvertex_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("SALEH", "d")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("SALEH", "d")),
        AndCondition(),
        timedelta(minutes=5)
    )
    pattern3 = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("TONY", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("SALEH", "d")),
        AndCondition(),
        timedelta(minutes=5)

    )

    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern1.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern3.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    selectivityMatrix = [[1.0, 0.15989723367389616], [1.0, 1.0]]
    arrivalRates = [0.013917884481558803, 0.012421711899791231]
    pattern2.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    patterns = [pattern1, pattern2, pattern3]
    state = patterns_initialize_function(patterns)
    pattern_to_tree_plan_map, shareable_pairs = state
    alg = algoA()
    alg.Nvertex_neighborhood(pattern_to_tree_plan_map, shareable_pairs, 3)
    print('Ok')
    return pattern_to_tree_plan_map


def run_all(tests: List[callable]):
    for test in tests:
        pattern_to_tree_plan_map = test()

        eval_mechanism_params = TreeBasedEvaluationMechanismParameters()
        unified_tree = TreeBasedEvaluationMechanism(pattern_to_tree_plan_map, eval_mechanism_params.storage_params,
                                                    eval_mechanism_params.multi_pattern_eval_params)
        unified_tree.visualize(title="SMT unified Tree")


if __name__ == '__main__':
    # Nedge_test()
    # pattern_to_tree_plan_map = annealing_med_test()
    #
    # eval_mechanism_params = TreeBasedEvaluationMechanismParameters()
    # unified_tree = TreeBasedEvaluationMechanism(pattern_to_tree_plan_map, eval_mechanism_params.storage_params,
    #                                             eval_mechanism_params.multi_pattern_eval_params)
    # unified_tree.visualize(title="SMT unified Tree")
    tests = [
        shareable_all_pairs_unit_test,
        Nedge_test,
        # annealing_basic_test()
        # annealing_med_test()
        Nvertex_test, ]
    run_all(tests=tests)
    create_topology_test()
    create_topology_const_sub_pattern_test()
    create_topology_sub_pattern_eq_pattern_test()
