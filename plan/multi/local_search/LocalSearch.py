from abc import ABC
from time import time
from random import sample, random, randint
from math import exp
from collections import deque

from plan.multi.local_search.MultiPatternGraph import MultiPatternGraph
from plan.multi.local_search.state_node import StateNode
from tree.MultiPatternTree import MultiPatternTree


class LocalSearch:

    def __init__(self, pattern_to_tree_plan_map,
                 eval_mechanism_params, steps_threshold, time_threshold):

        self._solution = StateNode(
            last_modified_node=None,
            mpg=MultiPatternGraph(list(pattern_to_tree_plan_map.keys())),
            mpt=MultiPatternTree(pattern_to_tree_plan_map=pattern_to_tree_plan_map,
                                 storage_params=eval_mechanism_params.storage_params),
            last_used=None
        )
        self._steps_threshold = steps_threshold
        self._time_threshold = time_threshold
        self.__was_activated = False

    def _make_step(self, solution):
        raise NotImplementedError()

    def _get_neighbors(self, state, size=None):
        # Todo: implement this
        raise NotImplementedError()

    def get_best_solution(self):
        if not self.__was_activated:
            self._solution, cost = self.__start_search()
            self.__was_activated = True
        return self._solution.mpt

    def _time_cond(self):
        return time() - self._running_time <= self._time_threshold

    def __start_search(self):
        allowed_steps = self._steps_threshold
        current_best_solution = self._solution
        current_best_cost = current_best_solution.get_cost()
        current_solution = self._solution
        self._running_time = time()
        while self._time_cond() and allowed_steps:
            current_solution = self._make_step(current_solution)
            if current_solution is None:
                break
            current_cost = current_solution.get_cost()
            if current_cost < current_best_cost:
                allowed_steps = self._steps_threshold
                current_best_solution = current_solution
                current_best_cost = current_cost
            else:
                allowed_steps -= 1

        return current_best_solution, current_best_cost


class TabuSearch(LocalSearch):
    def __init__(self, initial_capacity, lookup_radius, eval_mechanism_params,
                 pattern_to_tree_plan_map, steps_threshold, time_threshold):
        self.__capacity = initial_capacity
        self.__lookup_radius = lookup_radius
        self._tabu_list = deque()
        super().__init__(pattern_to_tree_plan_map=pattern_to_tree_plan_map,
                         steps_threshold=steps_threshold,
                         time_threshold=time_threshold,
                         eval_mechanism_params=eval_mechanism_params)

    def _make_step(self, state):
        tabu_set = set(self._tabu_list)
        flag = False
        while self._time_cond():
            # choosing a set until we get a non-empty one
            chosen_neighbors = set(self._get_neighbors(state, self.__lookup_radius))
            chosen_neighbors -= tabu_set
            if chosen_neighbors:
                flag = True
                break

        if not flag:
            return None

        cheapest_neighbor = min(chosen_neighbors, key=lambda sol: sol.get_cost())
        self._tabu_list.append(cheapest_neighbor)
        if len(self._tabu_list) > self.__capacity:
            self._tabu_list.popleft()

        return cheapest_neighbor


class SimulatedAnnealingSearch(LocalSearch):
    def __init__(self, pattern_to_tree_plan_map, eval_mechanism_params,
                 steps_threshold, time_threshold, alpha, c_0, c_threshold):
        self._c = c_0
        self._alpha = alpha
        self._c_threshold = c_threshold
        super().__init__(pattern_to_tree_plan_map=pattern_to_tree_plan_map,
                         steps_threshold=steps_threshold,
                         time_threshold=time_threshold,
                         eval_mechanism_params=eval_mechanism_params)

    def _make_step(self, state):
        if self._c < self._c_threshold:
            return None

        chosen = None
        cost = state.get_cost()
        while self._time_cond():
            neighbor = self._get_neighbors(state, size=1)
            if neighbor.get_cost() < cost:
                chosen = neighbor
                break
            else:
                # should accept with prob
                if random() < exp(-(neighbor.get_cost() - cost)/self._c):
                    chosen = neighbor
                    break

        self._c *= self._alpha
        return chosen
