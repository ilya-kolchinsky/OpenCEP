import random
import string
from datetime import timedelta
from typing import Tuple, Dict

from SimulatedAnnealing import SimulatedAnnealing
from base.PatternStructure import SeqOperator, PrimitiveEventStructure
from condition.BaseRelationCondition import GreaterThanCondition
from condition.CompositeCondition import AndCondition
from condition.Condition import Variable
from misc.StatisticsTypes import StatisticsTypes
from plan.MPT_neighborhood import algoA, TreePlan
from plan.TreeCostModel import TreePlanCostCalculator
from plan.TreePlanBuilderOrders import TreePlanBuilderOrder
from plan.UnifiedTreeBuilder import UnifiedTreeBuilder
from test.testUtils import *


# ------------------------------------ test utilities and auxiliary functions --------------------------
def tree_plan_equal(first_state: Tuple[Dict[Pattern, TreePlan], List[List]],
                    second_state: Tuple[Dict[Pattern, TreePlan], List[List]]):
    patterns = list(first_state[0].keys())

    leaves_dict = UnifiedTreeBuilder.get_pattern_leaves_dict(first_state[0])

    tree_plans1 = list([tree_plan for _, tree_plan in first_state[0].items()])
    tree_plans2 = list([tree_plan for _, tree_plan in second_state[0].items()])

    for idx, pattern in enumerate(patterns):
        tree_plans1_root = tree_plans1[idx].root
        tree_plans2_root = tree_plans2[idx].root
        if not UnifiedTreeBuilder.is_equivalent(tree_plans1_root, pattern, tree_plans2_root, pattern, leaves_dict):
            return False
    return True

def patterns_initialize_function(patterns: List[Pattern]):
    alg = algoA()
    pattern_to_tree_plan_map = {p: alg.create_tree_topology(p) for p in patterns}
    shareable_pairs = algoA.get_shareable_pairs(patterns)
    return pattern_to_tree_plan_map, shareable_pairs


def sharable_patterns_num(shareable_pairs):
    count_common_pairs = lambda lst: len(lst) if lst is not None else 0
    count_common_pairs_lst = lambda lst_of_lists: sum([count_common_pairs(lst) for lst in lst_of_lists])
    return sum([count_common_pairs_lst(lst_of_lists) for lst_of_lists in shareable_pairs])


def tree_plan_edge_neighbour(state: Tuple[Dict[Pattern, TreePlan], List[List]]):
    """Move a little bit x, from the left or the right."""
    pattern_to_tree_plan_map, shareable_pairs = state
    alg = algoA()
    if sharable_patterns_num(shareable_pairs) == 0:
        return state
    neighbour = alg.Nedge_neighborhood(pattern_to_tree_plan_map, shareable_pairs)
    return neighbour


def tree_plan_vertex_neighbour(state: Tuple[Dict[Pattern, TreePlan], List[List]]):
    """Move a little bit x, from the left or the right."""
    pattern_to_tree_plan_map, shareable_pairs = state
    alg = algoA()

    if sharable_patterns_num(shareable_pairs) == 0:
        return state
    neighbour = alg.Nvertex_neighborhood(pattern_to_tree_plan_map, shareable_pairs)
    return neighbour


def split_approach_string(approach: TreePlanBuilderOrder):
    return '{:10s}'.format(str(approach).split(".")[1])


def state_get_summary_aux(orders: List[int], approach: TreePlanBuilderOrder):
    return str(orders) + split_approach_string(approach)


def run_simulated_annealing(patterns: List[Pattern], initialize_function, state_equal_function, cost_function,
                            neighbour_function, state_repr_function=object.__str__, time_limit=100):
    simulated_annealing_instance = SimulatedAnnealing(patterns=patterns,
                                                      initialize_function=initialize_function,
                                                      cost_function=cost_function,
                                                      neighbour_function=neighbour_function,
                                                      state_equal_function=state_equal_function,
                                                      state_repr_function=state_repr_function,
                                                      time_limit=time_limit)

    state, c, states, costs = simulated_annealing_instance.timed_annealing()
    return state, c


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
    _ = algoA_instance._create_topology_with_const_sub_order(pattern1, [0, 3])


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

    return pattern_to_tree_plan_map


def Nedge_test_1():
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

    return pattern_to_tree_plan_map


def Nedge_test_2():
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
    selectivityMatrix = [[random.uniform(0, 1) for i in range(3)] for i in range(3)]
    arrivalRates = [random.uniform(0, 1) for i in range(3)]
    patterns[0].set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))

    selectivityMatrix = [[random.uniform(0, 1) for i in range(3)] for i in range(3)]
    arrivalRates = [random.uniform(0, 1) for i in range(3)]
    patterns[1].set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))

    selectivityMatrix = [[random.uniform(0, 1) for i in range(3)] for i in range(3)]
    arrivalRates = [random.uniform(0, 1) for i in range(3)]
    patterns[2].set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))

    state = patterns_initialize_function(patterns)
    pattern_to_tree_plan_map, shareable_pairs = state
    alg = algoA()
    alg.Nedge_neighborhood(pattern_to_tree_plan_map, shareable_pairs)

    return pattern_to_tree_plan_map


def annealing_basic_test_1():
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
    state, c = run_simulated_annealing(patterns=patterns,
                                       initialize_function=patterns_initialize_function,
                                       state_equal_function=tree_plan_equal,
                                       cost_function=TreePlanCostCalculator.tree_plan_cost_function,
                                       neighbour_function=tree_plan_edge_neighbour)
    state, c = run_simulated_annealing(patterns=patterns,
                                       initialize_function=patterns_initialize_function,
                                       state_equal_function=tree_plan_equal,
                                       cost_function=TreePlanCostCalculator.tree_plan_cost_function,
                                       neighbour_function=tree_plan_vertex_neighbour, time_limit=10000000)


def annealing_basic_test_2():
    events = ["AAPL", "AMZN", "GOOG", "FB"]
    patterns = []
    for i in range(0, 2):
        names = [''.join(random.choice(string.ascii_lowercase) for i in range(5)) for i in range(4)]
        random.shuffle(events)
        patterns.append(Pattern(
            SeqOperator(PrimitiveEventStructure(events[0], names[0]), PrimitiveEventStructure(events[0], names[1]),
                        PrimitiveEventStructure(events[2], names[2]), PrimitiveEventStructure(events[3], names[3])),
            AndCondition(
                GreaterThanCondition(Variable(names[0], lambda x: x["Peak Price"]), 135),
                GreaterThanCondition(Variable(names[0], lambda x: x["Opening Price"]),
                                     Variable(names[1], lambda x: x["Opening Price"])),
                GreaterThanCondition(Variable(names[1], lambda x: x["Opening Price"]),
                                     Variable(names[2], lambda x: x["Opening Price"]))),
            timedelta(minutes=5)
        ))
    selectivityMatrix = [[random.uniform(0, 1) for i in range(4)] for i in range(4)]
    arrivalRates = [random.uniform(0, 1) for i in range(4)]
    patterns[0].set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))

    selectivityMatrix = [[random.uniform(0, 1) for i in range(4)] for i in range(4)]
    arrivalRates = [random.uniform(0, 1) for i in range(4)]
    patterns[1].set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))

    state, c = run_simulated_annealing(patterns=patterns,
                                       initialize_function=patterns_initialize_function,
                                       state_equal_function=tree_plan_equal,
                                       cost_function=TreePlanCostCalculator.tree_plan_cost_function,
                                       neighbour_function=tree_plan_edge_neighbour,
                                       time_limit=10)
    state, c = run_simulated_annealing(patterns=patterns,
                                       initialize_function=patterns_initialize_function,
                                       state_equal_function=tree_plan_equal,
                                       cost_function=TreePlanCostCalculator.tree_plan_cost_function,
                                       neighbour_function=tree_plan_vertex_neighbour,
                                       time_limit=10)


def annealing_med_test_1():
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
    state, c = run_simulated_annealing(patterns=patterns,
                                       initialize_function=patterns_initialize_function,
                                       state_equal_function=tree_plan_equal,
                                       cost_function=TreePlanCostCalculator.tree_plan_cost_function,
                                       neighbour_function=tree_plan_edge_neighbour,
                                       time_limit=10)
    state, c = run_simulated_annealing(patterns=patterns,
                                       initialize_function=patterns_initialize_function,
                                       state_equal_function=tree_plan_equal,
                                       cost_function=TreePlanCostCalculator.tree_plan_cost_function,
                                       neighbour_function=tree_plan_vertex_neighbour,
                                       time_limit=10)


def annealing_med_test_2():
    events = ["AAPL", "AMZN", "GOOG", "FB", "MIC"]
    patterns = []
    for i in range(0, 8):
        names = [''.join(random.choice(string.ascii_lowercase) for i in range(5)) for i in range(4)]
        random.shuffle(events)
        patterns.append(Pattern(
            SeqOperator(PrimitiveEventStructure(events[0], names[0]), PrimitiveEventStructure(events[0], names[1]),
                        PrimitiveEventStructure(events[2], names[2]), PrimitiveEventStructure(events[3], names[3])),
            AndCondition(
                GreaterThanCondition(Variable(names[0], lambda x: x["Peak Price"]), 135),
                GreaterThanCondition(Variable(names[0], lambda x: x["Opening Price"]),
                                     Variable(names[1], lambda x: x["Opening Price"])),
                GreaterThanCondition(Variable(names[1], lambda x: x["Opening Price"]),
                                     Variable(names[2], lambda x: x["Opening Price"]))),
            timedelta(minutes=5)
        ))
    for i in range(8):
        selectivityMatrix = [[random.uniform(0, 1) for i in range(4)] for i in range(4)]
        arrivalRates = [random.uniform(0, 1) for i in range(4)]
        patterns[i].set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES,
                                   (selectivityMatrix, arrivalRates))
    state, c = run_simulated_annealing(patterns=patterns,
                                       initialize_function=patterns_initialize_function,
                                       state_equal_function=tree_plan_equal,
                                       cost_function=TreePlanCostCalculator.tree_plan_cost_function,
                                       neighbour_function=tree_plan_vertex_neighbour,
                                       time_limit=10)


def Nvertex_test_1():
    events = ["AAPL", "AMZN", "GOOG", "FB", "MIC"]
    patterns = []
    for i in range(0, 20):
        names = [''.join(random.choice(string.ascii_lowercase) for i in range(5)) for i in range(4)]
        random.shuffle(events)
        patterns.append(Pattern(
            SeqOperator(PrimitiveEventStructure(events[0], names[0]), PrimitiveEventStructure(events[0], names[1]),
                        PrimitiveEventStructure(events[2], names[2]), PrimitiveEventStructure(events[3], names[3])),
            AndCondition(
                GreaterThanCondition(Variable(names[0], lambda x: x["Peak Price"]), 135),
                GreaterThanCondition(Variable(names[0], lambda x: x["Opening Price"]),
                                     Variable(names[1], lambda x: x["Opening Price"])),
                GreaterThanCondition(Variable(names[1], lambda x: x["Opening Price"]),
                                     Variable(names[2], lambda x: x["Opening Price"]))),
            timedelta(minutes=5)
        ))
    for i in range(20):
        selectivityMatrix = [[random.uniform(0, 1) for i in range(4)] for i in range(4)]
        arrivalRates = [random.uniform(0, 1) for i in range(4)]
        patterns[i].set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES,
                                   (selectivityMatrix, arrivalRates))
    state = patterns_initialize_function(patterns)
    pattern_to_tree_plan_map, shareable_pairs = state
    alg = algoA()
    alg.Nvertex_neighborhood(pattern_to_tree_plan_map, shareable_pairs, 3)


def Nvertex_test_2():
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

    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0],
                         [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864],
                         [1.0, 1.0, 0.9992557393942864, 1.0]]
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

    return pattern_to_tree_plan_map


def advanced_Nvertex_no_conditions_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("SALEH", "d")),
        AndCondition(),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"),
                    PrimitiveEventStructure("TONY", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("SALEH", "d")),
        AndCondition(),
        timedelta(minutes=5)

    )
    pattern3 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("SALEH", "d")),
        AndCondition(),
        timedelta(minutes=5)
    )
    pattern4 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(),
        timedelta(minutes=5)
    )

    pattern5 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("FB", "e")),
        AndCondition(),
        timedelta(minutes=5)
    )

    pattern6 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("LI", "c")),
        AndCondition(),
        timedelta(minutes=2)
    )
    pattern7 = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"),
                    PrimitiveEventStructure("TONY", "b"),
                    PrimitiveEventStructure("FB", "e")),
        AndCondition(),
        timedelta(minutes=5)
    )

    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0],
                         [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864],
                         [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern1.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern2.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    selectivityMatrix = [[1.0, 0.15989723367389616],
                         [1.0, 1.0]]
    arrivalRates = [0.013917884481558803, 0.012421711899791231]
    pattern3.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0],
                         [0.9457796098355941, 1.0, 0.15989723367389616],
                         [1.0, 0.15989723367389616, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803]
    pattern4.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern5.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern6.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern7.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))

    patterns = [pattern1, pattern2, pattern3, pattern4, pattern5, pattern6, pattern7]
    state = patterns_initialize_function(patterns)
    pattern_to_tree_plan_map, shareable_pairs = state
    alg = algoA()
    alg.Nvertex_neighborhood(pattern_to_tree_plan_map, shareable_pairs, 7)

    return pattern_to_tree_plan_map


def advanced_Nvertex_test():
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
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("TONY", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("SALEH", "d")),
        AndCondition(),
        timedelta(minutes=5)

    )
    pattern3 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("SALEH", "d")),
        AndCondition(),
        timedelta(minutes=5)
    )
    pattern4 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("c", lambda x: x["Peak Price"]), 500)
        ),
        timedelta(minutes=5)
    )

    pattern5 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("c", lambda x: x["Peak Price"]), 530)
        ),
        timedelta(minutes=3)
    )

    pattern6 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("FB", "e")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("e", lambda x: x["Peak Price"]), 520)
        ),
        timedelta(minutes=5)
    )

    pattern7 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("LI", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("c", lambda x: x["Peak Price"]), 100)
        ),
        timedelta(minutes=2)
    )
    pattern8 = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("TONY", "b"),
                    PrimitiveEventStructure("FB", "e")),
        AndCondition(),
        timedelta(minutes=5)
    )
    pattern9 = Pattern(
        SeqOperator(PrimitiveEventStructure("FB", "e"),
                    PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("TONY", "b")),
        AndCondition(),
        timedelta(minutes=5)
    )

    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0],
                         [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864],
                         [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern1.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern2.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    selectivityMatrix = [[1.0, 0.15989723367389616],
                         [1.0, 1.0]]
    arrivalRates = [0.013917884481558803, 0.012421711899791231]
    pattern3.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0],
                         [0.9457796098355941, 1.0, 0.15989723367389616],
                         [1.0, 0.15989723367389616, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803]
    pattern4.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern5.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern6.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern7.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern8.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern9.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    patterns = [pattern1, pattern2, pattern3, pattern4, pattern5, pattern6, pattern7, pattern8, pattern9]
    state = patterns_initialize_function(patterns)
    pattern_to_tree_plan_map, shareable_pairs = state
    alg = algoA()
    pattern_to_tree_plan_map, _ = alg.Nvertex_neighborhood(pattern_to_tree_plan_map, shareable_pairs, 9)

    return pattern_to_tree_plan_map


def run_all(tests: List[callable]):
    for test in tests:
        print(f'{test.__name__:35s}', end='\t')
        test()
        print('SUCCESS')


def run_MPT_Neighborhood_Tests():
    tests = [
        shareable_all_pairs_unit_test,
        Nedge_test_1,
        Nedge_test_2,
        annealing_basic_test_1,
        annealing_basic_test_2,
        annealing_med_test_1,
        annealing_med_test_2,
        Nvertex_test_1,
        Nvertex_test_2,
        advanced_Nvertex_no_conditions_test,
        advanced_Nvertex_test,
    ]
    run_all(tests=tests)
    create_topology_test()
    create_topology_const_sub_pattern_test()
    create_topology_sub_pattern_eq_pattern_test()
