import sys
import traceback
from datetime import datetime
from typing import Dict, Tuple, List

import matplotlib as mpl
import matplotlib.pyplot as plt  # to plot
import numpy as np
import numpy.random as rn
import seaborn as sns
import tqdm

from base.Pattern import Pattern
from plan.TreeCostModel import TreeCostModelFactory
from plan.TreePlan import TreePlan
from plan.TreePlanBuilder import TreePlanBuilder
from plan.TreePlanBuilderOrders import TreePlanBuilderOrder

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
    plt.title(title)
    plt.subplots_adjust(top=0.85)
    plt.show()


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
    T = 100
    no_improve_steps = 0

    start_time = datetime.now()

    with tqdm.tqdm(total=max_steps, file=sys.stdout) as pbar:
        while True:
            try:
                for step in range(2, max_steps):
                    if (datetime.now() - start_time).total_seconds() > time_limit or T <= 0.05:
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
                              neighbour_function, time_limit=100):
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


# ======================================================================

def split_approach_string(approach: TreePlanBuilderOrder):
    return '{:10s}'.format(str(approach).split(".")[1])


def state_get_summary_aux(orders: List[int], approach: TreePlanBuilderOrder):
    return str(orders) + split_approach_string(approach)


def state_get_summary(pattern_to_tree_plan_map: Dict[Pattern, Tuple[TreePlan, List[int], TreePlanBuilderOrder]]):
    return "".join([state_get_summary_aux(orders, approach) + '\n' for _, (_, orders, approach) in
                    pattern_to_tree_plan_map.items()])


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
        if not TreePlanBuilder.is_equivalent(tree_plans1_root, pattern, tree_plans2_root, pattern, leaves_dict):
            return False
    return True


