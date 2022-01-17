from abc import ABC
from time import time
from random import sample, random
from math import exp
from collections import deque


class LocalSearch:

    def __init__(self, initial_solution, steps_threshold, time_threshold):
        self._solution = initial_solution
        self._steps_threshold = steps_threshold
        self._time_threshold = time_threshold
        self.__was_activated = False

    def make_step(self, solution):
        raise NotImplementedError()

    def get_neighbors(self, state):
        # Todo: implement this
        raise NotImplementedError()

    def get_best_solution(self):
        if not self.__was_activated:
            self._solution, cost = self.__start_search()
            self.__was_activated = True
        return self._solution

    def __start_search(self):
        allowed_steps = self._steps_threshold
        current_best_solution = self._solution
        current_best_cost = current_best_solution.get_cost()
        current_solution = self._solution
        running_time = time()
        while time() - running_time <= self._time_threshold and allowed_steps:
            current_solution = self.make_step(current_solution)
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
    def __init(self, initial_capacity, lookup_radius, initial_solution, steps_threshold, time_threshold):
        self.__capacity = initial_capacity
        self.__lookup_radius = lookup_radius
        self._tabu_list = deque()
        super().__init__(initial_solution=initial_solution,
                         steps_threshold=steps_threshold,
                         time_threshold=time_threshold)

    def make_step(self, state):
        neighbors = self.get_neighbors(state)
        tabu_set = set(self._tabu_list)
        while True:
            # choosing a set until we get a non-empty one
            chosen_neighbors = set(sample(neighbors, min(self.__lookup_radius, len(neighbors))))
            chosen_neighbors -= tabu_set
            if chosen_neighbors:
                break

        cheapest_neighbor = min(chosen_neighbors, key=lambda sol: sol.get_cost())
        self._tabu_list.append(cheapest_neighbor)
        if len(self._tabu_list) > self.__capacity:
            self._tabu_list.popleft()

        return cheapest_neighbor


class SimulatedAnnealingSearch(LocalSearch):
    def __init__(self, initial_solution, steps_threshold, time_threshold, alpha, c_0, c_threshold):
        self._c = c_0
        self._alpha = alpha
        self._c_threshold = c_threshold
        super().__init__(initial_solution=initial_solution,
                         steps_threshold=steps_threshold,
                         time_threshold=time_threshold)

    def make_step(self, state):
        if self._c < self._c_threshold:
            return None

        cost = state.get_cost()
        chosen, best_cost = None, cost
        for neighbor in self.get_neighbors(state):
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
