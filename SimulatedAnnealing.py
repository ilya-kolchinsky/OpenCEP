import math
import sys
import traceback
from datetime import datetime
from typing import Dict, Tuple, List
import numpy.random as rn

from base.Pattern import Pattern
from plan.TreePlan import TreePlan
from plan.TreePlanBuilder import TreePlanBuilder
from plan.TreePlanBuilderOrders import TreePlanBuilderOrder


class SimulatedAnnealing:

    def __init__(self, patterns: List[Pattern], initialize_function, state_equal_function, state_repr_function, cost_function,
                 neighbour_function, time_limit=100):
        self.patterns = patterns
        self.initialize_function = initialize_function
        self.state_equal_function = state_equal_function
        self.state_repr_function = state_repr_function
        self.cost_function = cost_function
        self.neighbour_function = neighbour_function
        self.time_limit = time_limit

    @staticmethod
    def acceptance(cost, new_cost, temperature):
        if new_cost < cost:
            return 1
        else:
            p = math.exp(- (new_cost - cost) / temperature)
            return p

    @staticmethod
    def update_temperature(T, fraction=0.95):
        """ Example of temperature dicreasing as the process goes on."""
        return max(0.05, T * fraction)

    @staticmethod
    def tree_plan_is_equivalent(first_pattern_to_tree_plan_map: Dict[Pattern, Tuple[TreePlan, List[int], TreePlanBuilderOrder]],
                                second_pattern_to_tree_plan_map: Dict[Pattern, Tuple[TreePlan, List[int], TreePlanBuilderOrder]]):
        leaves_dict = {}

        patterns = list(first_pattern_to_tree_plan_map.keys())
        for i, pattern in enumerate(patterns):
            tree_plan, _, _ = first_pattern_to_tree_plan_map[pattern]
            tree_plan_leaves_pattern = tree_plan.root.get_leaves()
            pattern_args = pattern.positive_structure.get_args()
            pattern_event_size = len(pattern_args)
            leaves_dict[pattern] = {tree_plan_leaves_pattern[i]: pattern_args[tree_plan_leaves_pattern[0].event_index] for i
                                    in
                                    range(pattern_event_size)}

        tree_plans1 = list([tree_plan for _, (tree_plan, _, _) in first_pattern_to_tree_plan_map.items()])
        tree_plans2 = list([tree_plan for _, (tree_plan, _, _) in second_pattern_to_tree_plan_map.items()])

        for idx, pattern in enumerate(patterns):
            tree_plans1_root = tree_plans1[idx].root
            tree_plans2_root = tree_plans2[idx].root
            if not TreePlanBuilder.is_equivalent(tree_plans1_root, pattern, tree_plans2_root, pattern, leaves_dict):
                return False
        return True

    def timed_annealing(self, max_steps=1000, debug=True):
        """ Optimize the black-box function 'cost_function' with the simulated annealing algorithm."""
        state = self.initialize_function(self.patterns)
        cost = self.cost_function(state)
        states, costs = [1], [cost]
        T = 50
        no_improve_steps = 0

        start_time = datetime.now()

        while True:
            try:
                for step in range(2, max_steps):
                    if (datetime.now() - start_time).total_seconds() > self.time_limit or T <= 0.05:
                        return state, self.cost_function(state), states, costs
                    fraction = step / float(max_steps)
                    T = self.update_temperature(T)
                    new_state = self.neighbour_function(state)
                    new_cost = self.cost_function(new_state)

                    if self.state_equal_function(new_state, state):
                        return state, self.cost_function(state), states, costs

                    if self.acceptance(cost, new_cost, T) > rn.random():
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

            return state, self.cost_function(state), states, costs
