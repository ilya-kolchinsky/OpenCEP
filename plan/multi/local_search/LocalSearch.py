from abc import ABC
from time import time
from random import random
from math import exp
from collections import deque
from typing import Dict

from adaptive.optimizer.Optimizer import Optimizer
from base.Pattern import Pattern
from plan.TreePlan import TreePlan
from plan.multi.local_search.MultiPatternGraph import MultiPatternGraph
from plan.multi.local_search.StateNode import StateNode


class LocalSearch(ABC):

    def __init__(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan], optimizer: Optimizer, steps_threshold: int,
                 time_threshold: float, neighborhood_vertex_size: int):
        self._solution = StateNode(
            mpg=MultiPatternGraph(list(pattern_to_tree_plan_map.keys())),
            pattern_to_tree_plan_map=pattern_to_tree_plan_map,
            optimizer=optimizer,
        )
        self._steps_threshold = steps_threshold
        self._time_threshold = time_threshold
        self.__neighborhood_vertex_size = neighborhood_vertex_size
        self.__was_activated = False

    def _make_step(self, state):
        raise NotImplementedError()

    def _get_neighbors(self, state: StateNode, size: int):
        neighbors = []
        for i in range(size):
            while self._time_cond():
                neighbor = state.get_neighbor(self.__neighborhood_vertex_size)
                if neighbor is not None:
                    neighbors.append(neighbor)
                    break
            if not self._time_cond():
                break
        return neighbors

    def get_best_solution(self):
        if not self.__was_activated:
            self._solution, cost = self._start_search()
            self.__was_activated = True
        return self._solution.pattern_to_tree_plan_map

    def _time_cond(self):
        return time() - self._running_time <= self._time_threshold

    def _start_search(self):
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
    def __init__(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan], optimizer: Optimizer, steps_threshold: int,
                 time_threshold: float, neighborhood_vertex_size: int, capacity: int, lookup_radius: int):
        super().__init__(pattern_to_tree_plan_map=pattern_to_tree_plan_map, optimizer=optimizer,
                         steps_threshold=steps_threshold, time_threshold=time_threshold,
                         neighborhood_vertex_size=neighborhood_vertex_size
                         )
        self.__capacity = capacity
        self.__lookup_radius = lookup_radius
        self._tabu_list = deque()

    def _make_step(self, state):
        tabu_set = set(self._tabu_list)
        flag = False
        chosen_neighbors = None
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
        self._tabu_list.extend(chosen_neighbors)
        if len(self._tabu_list) > self.__capacity:
            self._tabu_list.popleft()

        return cheapest_neighbor


class SimulatedAnnealingSearch(LocalSearch):
    def __init__(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan], optimizer: Optimizer, steps_threshold: int,
                 time_threshold: float, neighborhood_vertex_size: int, multiplier: float,
                 simulated_anealing_threshold: float, initial_neighbors: int):
        super().__init__(pattern_to_tree_plan_map=pattern_to_tree_plan_map,
                         optimizer=optimizer, steps_threshold=steps_threshold, time_threshold=time_threshold,
                         neighborhood_vertex_size=neighborhood_vertex_size)
        self._alpha = multiplier
        self._c_threshold = simulated_anealing_threshold
        # Initialize C0 according to the article
        neighbors = self._get_neighbors(self._solution, initial_neighbors)
        if not neighbors:
            raise Exception('Failed initializing Simulated Annealing')
        init_solution_cost = self._solution.get_cost()
        # Set C0, the initial value, to the largest difference observed with the calculated neighbors of initial state
        c_0 = max([abs(init_solution_cost - neighbor.get_cost()) for neighbor in neighbors])
        self._c = c_0

    def _make_step(self, state):
        if self._c < self._c_threshold:
            return None

        chosen = None
        cost = state.get_cost()
        while self._time_cond():
            neighbors = self._get_neighbors(state, size=1)
            if not neighbors:
                continue
            neighbor = neighbors[0]
            if neighbor.get_cost() < cost:
                chosen = neighbor
                break
            else:
                # should choose non-improving solution with some probability
                if random() < exp(-(neighbor.get_cost() - cost)/self._c):
                    chosen = neighbor
                    break

        self._c *= self._alpha
        return chosen
