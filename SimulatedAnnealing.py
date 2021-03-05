import math
import math
import traceback
from datetime import datetime
from random import random
from typing import Dict, Tuple, List

from base.Pattern import Pattern
from plan.MPT_neighborhood import algoA
from plan.TreeCostModel import TreePlanCostCalculator
from plan.TreePlan import TreePlan
from plan.TreePlanBuilder import TreePlanBuilder
from plan.TreePlanBuilderOrders import TreePlanBuilderOrder
from plan.UnifiedTreeBuilder import UnifiedTreeBuilder
from plan.multi.MultiPatternUnifiedTreeLocalSearchApproaches import MultiPatternUnifiedTreeLocalSearchApproaches


class SimulatedAnnealing:

    def __init__(self, patterns: List[Pattern],
                 initialize_function,
                 state_equal_function,
                 cost_function,
                 neighbour_function,
                 state_repr_function=object.__str__,
                 time_limit=100):
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

                    if self.acceptance(cost, new_cost, T) > random():
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

    @staticmethod
    def get_neighbour_function(local_search_neighbor_approach):
        def _neighbour_function(state: Tuple[Dict[Pattern, TreePlan], List[List]]):
            pattern_to_tree_plan_map, shareable_pairs = state
            alg = algoA()
            if sharable_patterns_num(shareable_pairs) == 0:
                return state
            if local_search_neighbor_approach == MultiPatternUnifiedTreeLocalSearchApproaches.EDGE_NEIGHBOR:
                neighbour = alg.Nedge_neighborhood(pattern_to_tree_plan_map, shareable_pairs)
                return neighbour
            elif local_search_neighbor_approach == MultiPatternUnifiedTreeLocalSearchApproaches.VERTEX_NEIGHBOR:
                neighbour = alg.Nvertex_neighborhood(pattern_to_tree_plan_map, shareable_pairs)
                return neighbour
            else:
                raise Exception("Unsupported local search successor function")

        return _neighbour_function

    @staticmethod
    def patterns_initialize_function(patterns: List[Pattern]):
        alg = algoA()
        pattern_to_tree_plan_map = {p: alg.create_tree_topology(p) for p in patterns}
        shareable_pairs = algoA.get_shareable_pairs(patterns)
        return pattern_to_tree_plan_map, shareable_pairs

    @staticmethod
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

    @staticmethod
    def construct_subtrees_local_search_tree_plan(pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan,
                                                  tree_plan_local_search_params: Tuple[
                                                      MultiPatternUnifiedTreeLocalSearchApproaches, int]
                                                  ):
        """
        @param pattern_to_tree_plan_map :dict between the Patter and his treePlan
        @param tree_plan_local_search_params: a tuple of 2:
                    * first entry is the local_search_neighbor_approach either Nedge or Nvertex approach
                    * second entry is the time limit for the local search to run
        @return: the new pattern_to_tree_plan_map
                This method gets patterns, builds a single-pattern tree to each one of them,
                and merges equivalent subtrees from different trees using simulated annealing local search algorithm.
        """
        local_search_neighbor_approach, time_limit = tree_plan_local_search_params
        patterns = list(pattern_to_tree_plan_map.keys())
        neighbour_function = SimulatedAnnealing.get_neighbour_function(local_search_neighbor_approach)
        simulated_annealing_instance = SimulatedAnnealing(patterns=patterns,
                                                          initialize_function=SimulatedAnnealing.patterns_initialize_function,
                                                          cost_function=TreePlanCostCalculator.tree_plan_cost_function,
                                                          neighbour_function=neighbour_function,
                                                          state_equal_function=SimulatedAnnealing.tree_plan_equal,
                                                          time_limit=time_limit)

        state, c, states, costs = simulated_annealing_instance.timed_annealing()
        pattern_to_tree_plan_map, _ = state
        return pattern_to_tree_plan_map








