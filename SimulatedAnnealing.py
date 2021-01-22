import time
import traceback
from datetime import timedelta, date, datetime
from typing import Dict, Tuple

import matplotlib as mpl
import matplotlib.pyplot as plt  # to plot
import numpy as np
import numpy.random as rn
import seaborn as sns
import tqdm
from base.PatternStructure import SeqOperator, PrimitiveEventStructure
from condition.BaseRelationCondition import GreaterThanCondition
from condition.CompositeCondition import AndCondition
from condition.Condition import Variable
from misc import DefaultConfig
from misc.StatisticsTypes import StatisticsTypes
from plan.TreeCostModel import TreeCostModelFactory
from plan.TreePlan import TreePlan, TreePlanLeafNode
from test.testUtils import *
from tree.TreeVisualizationUtility import GraphVisualization

sns.set(context="talk", style="darkgrid", palette="hls", font="sans-serif", font_scale=1.05)

FIGSIZE = (19, 8)  #: Figure size, in inches!
mpl.rcParams['figure.figsize'] = FIGSIZE

interval = (-10, 10)


def acceptance_probability(cost, new_cost, temperature):
    if new_cost < cost:
        return 1
    else:
        p = np.exp(- (new_cost - cost) / temperature)
        return p


def update_temperature(T, fraction=0.95):
    """ Example of temperature dicreasing as the process goes on."""
    return max(0.05, T * fraction)


def see_annealing(states, costs, title="Evolution of states and costs of the simulated annealing"):

    plt.title("States")
    plt.xlabel("Step")
    plt.plot(costs, 'b')
    plt.title("Costs")
    plt.subplots_adjust(top=0.85)
    plt.show()


def tree_plan_annealing(
        patterns: List[Pattern],
        random_start,
        cost_function,
        random_neighbour,
        state_equal_function,
        state_repr_function,
        acceptance=acceptance_probability,
        temperature=update_temperature,
        max_steps=1000,
        debug=True):
    """ Optimize the black-box function 'cost_function' with the simulated annealing algorithm."""
    state = random_start(patterns)
    cost = cost_function(state)
    states, costs = [1], [cost]
    T = 0.95
    no_improve_steps = 0
    with tqdm.tqdm(total=max_steps, file=sys.stdout) as pbar:
        for step in range(2, max_steps):
            fraction = step / float(max_steps)
            T = temperature(T)
            new_state = random_neighbour(state)
            new_cost = cost_function(new_state)
            pbar.update()
            if debug: print(
                "Step #{:>2}/{:>2} : T = {:>4.3g}, state = {:>4.3g}, cost = {:>4.3g}, new_state = {:>4.3g}, new_cost = {:>4.3g} ...".format(
                    step,
                    max_steps,
                    T,
                    state,
                    cost,
                    new_state,
                    new_cost))

            if state_equal_function(new_state, state):
                return state, cost_function(state), states, costs

            if acceptance(cost, new_cost, T) > rn.random():
                state, cost = new_state, new_cost
                states.append(step)
                costs.append(cost)
                no_improve_steps = 0
            else:
                no_improve_steps += 1

    return state, cost_function(state), states, costs


def timed_annealing(patterns: List[Pattern],
                    random_start,
                    cost_function,
                    random_neighbour,
                    state_equal_function,
                    state_repr_function,
                    acceptance=acceptance_probability,
                    temperature=update_temperature,
                    time_limit=5,
                    max_steps=1000,
                    debug=True):
    """ Optimize the black-box function 'cost_function' with the simulated annealing algorithm."""
    state = random_start(patterns)
    cost = cost_function(state)
    states, costs = [1], [cost]
    T = 0.95
    no_improve_steps = 0

    start_time = datetime.now()

    with tqdm.tqdm(total=max_steps, file=sys.stdout) as pbar:
        while True:
            try:
                for step in range(2, max_steps):
                    if (datetime.now() - start_time).seconds > time_limit or T <= 0.05:
                        return state, cost_function(state), states, costs

                    pbar.update()
                    fraction = step / float(max_steps)
                    T = temperature(T)
                    new_state = random_neighbour(state)
                    new_cost = cost_function(new_state)

                    if state_equal_function(new_state, state):
                        return state, cost_function(state), states, costs

                    if acceptance(cost, new_cost, T) > rn.random():
                        state, cost = new_state, new_cost
                        states.append(step)
                        costs.append(cost)
                        no_improve_steps = 0
                    else:
                        no_improve_steps += 1

            except:
                if state is None:
                    traceback.print_exc()
                    raise Exception(f"result is None")

    return state, cost_function(state), states, costs


def visualize_annealing_timed(patterns: List[Pattern], initialize_function, state_equal_function, state_repr_function, cost_function,
                              neighbour_function, time_limit):
    state, c, states, costs = timed_annealing(patterns=patterns,
                                              random_start=initialize_function,
                                              cost_function=cost_function,
                                              random_neighbour=neighbour_function,
                                              state_equal_function=state_equal_function,
                                              state_repr_function=state_repr_function,
                                              acceptance=acceptance_probability,
                                              time_limit=time_limit,
                                              max_steps=1000,
                                              debug=False)
    see_annealing(states, costs, title="Evolution of states and costs of the time limited simulated annealing")
    return state, c


def tree_plan_visualize_annealing(patterns: List[Pattern], initialize_function, state_equal_function, state_repr_function, cost_function,
                                  neighbour_function):
    state, c, states, costs = tree_plan_annealing(patterns=patterns,
                                                  random_start=initialize_function,
                                                  cost_function=cost_function,
                                                  random_neighbour=neighbour_function,
                                                  state_equal_function=state_equal_function,
                                                  state_repr_function=state_repr_function,
                                                  max_steps=100000,
                                                  debug=False)
    see_annealing(states, costs)
    return state, c


# ======================================================================
def rand_bin_array(n):
    arr = np.zeros(n)
    K = np.random.randint(low=0, high=n)
    arr[:K] = 1
    np.random.shuffle(arr)
    return arr


def random_tree_plan_neighbour(
        pattern_to_tree_plan_map: Dict[Pattern, Tuple[TreePlan, List[int], TreePlanBuilderOrder]]):
    """Move a little bit x, from the left or the right."""

    builders = UnifiedTreeBuilder.create_ordered_tree_builders()
    tree_plan_build_approaches = builders.keys()

    for pattern, (tree_plan, _, _) in pattern_to_tree_plan_map.items():
        random_builder_approach = np.random.choice(list(tree_plan_build_approaches))
        builder = builders.get(random_builder_approach)
        args_num = len(pattern.positive_structure.args)
        order = list(range(args_num))
        np.random.shuffle(order)
        tree = TreePlan(builder._order_to_tree_topology(order, pattern))
        tree_copy = tree
        pattern_to_tree_plan_map[pattern] = (tree_copy, order, random_builder_approach)

    return pattern_to_tree_plan_map


def split_approach_string(approach: TreePlanBuilderOrder):
    return '{:10s}'.format(str(approach).split(".")[1])


def state_get_summary_aux(orders: List[int], approach: TreePlanBuilderOrder):
    return str(orders) + split_approach_string(approach)


def state_get_summary(pattern_to_tree_plan_map: Dict[Pattern, Tuple[TreePlan, List[int], TreePlanBuilderOrder]]):
    return "".join([state_get_summary_aux(orders, approach) + '\n' for _, (_, orders, approach) in
                    pattern_to_tree_plan_map.items()])


def random_tree_start(pattern: Pattern):
    builders = UnifiedTreeBuilder.create_ordered_tree_builders()
    tree_plan_build_approaches = builders.keys()

    builder = builders.get(np.random.choice(list(tree_plan_build_approaches)))
    order = list(range(len(pattern.positive_structure.args)))
    np.random.shuffle(order)
    pattern_tree_plan = TreePlan(builder._order_to_tree_topology(order, pattern))
    return pattern_tree_plan, order, builder.tree_plan_build_approach


def visualize(tree_plan: TreePlan, title=None, visualize_flag=DefaultConfig.VISUALIZATION):
    if visualize_flag:
        G = GraphVisualization(title)
        G.build_from_root_treePlan(tree_plan.root, node_level=tree_plan.height)
        G.visualize()


def get_default_pattern():
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("GOOG", "e")),
        AndCondition(
            GreaterThanCondition(Variable("a", get_opening_price),
                                 Variable("b", get_opening_price)),
            GreaterThanCondition(Variable("c", get_peak_price), 503)
        ),
        timedelta(minutes=5)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    return pattern


def single_pattern_annealing_unit_test():
    pattern = get_default_pattern()

    tree_plan, order, tree_plan_build_approach = random_tree_start(pattern)
    cost_model = TreeCostModelFactory.create_cost_model()
    cost = cost_model.get_plan_cost(pattern, tree_plan.root)
    print(f'init cost = {cost}')
    for i in range(10):
        pattern_to_tree_plan_map = random_tree_plan_neighbour({pattern: (tree_plan, order, tree_plan_build_approach)})
        cost = cost_model.get_plan_cost(pattern, pattern_to_tree_plan_map[pattern][0].root)
        print(f'cost {i + 1:2d} = {cost}')


def patterns_random_initialize_function(patterns: List[Pattern]):
    pattern_to_tree_plan_map = {p: random_tree_start(p) for p in patterns}
    return pattern_to_tree_plan_map


def tree_plan_cost_function(pattern_to_tree_plan_map: Dict[Pattern, Tuple[TreePlan, List[int], TreePlanBuilderOrder]]):
    cost_model = TreeCostModelFactory.create_cost_model()
    tree_plan_total_cost = sum([cost_model.get_plan_cost(pattern, tree_plan.root) for pattern, (tree_plan, _, _) in
                                pattern_to_tree_plan_map.items()])
    return tree_plan_total_cost


def tree_plan_equal(pattern_to_tree_plan_map1: Dict[Pattern, Tuple[TreePlan, List[int], TreePlanBuilderOrder]],
                    pattern_to_tree_plan_map2: Dict[Pattern, Tuple[TreePlan, List[int], TreePlanBuilderOrder]]):
    leaves_dict = {}

    patterns = list(pattern_to_tree_plan_map1.keys())
    for i, pattern in enumerate(patterns):
        tree_plan, _, _ = pattern_to_tree_plan_map1[pattern]
        tree_plan_leaves_pattern = tree_plan.root.get_leaves()
        pattern_args = pattern.positive_structure.get_args()
        pattern_event_size = len(pattern_args)
        leaves_dict[pattern] = {tree_plan_leaves_pattern[i]: pattern_args[tree_plan_leaves_pattern[0].event_index] for i
                                in
                                range(pattern_event_size)}

    tree_plans1 = list([tree_plan for _, (tree_plan, _, _) in pattern_to_tree_plan_map1.items()])
    tree_plans2 = list([tree_plan for _, (tree_plan, _, _) in pattern_to_tree_plan_map2.items()])

    for idx, pattern in enumerate(patterns):
        tree_plans1_root = tree_plans1[idx].root
        tree_plans2_root = tree_plans2[idx].root
        if not UnifiedTreeBuilder.is_equivalent(tree_plans1_root, pattern, tree_plans2_root, pattern, leaves_dict):
            return False
    return True


