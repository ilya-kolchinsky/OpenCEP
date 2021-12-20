from abc import ABC
from time import time
from random import sample
from collections import deque


class LocalSearch(ABC):

    def __init__(self, initial_solution, steps_threshold, time_threshold):
        self._solution = initial_solution
        self._steps_threshold = steps_threshold
        self._time_threshold = time_threshold

    def make_step(self, solution):
        raise NotImplementedError()

    def get_neighbors(self, state):
        # Todo: implement this
        raise NotImplementedError()

    def get_best_solution(self):
        pass

    def start_search(self):
        raise NotImplementedError()


class TabuSearch(LocalSearch):
    def __init(self, initial_capacity, lookup_radius, initial_solution, steps_threshold, time_threshold):
        self.__capacity = initial_capacity
        self.__lookup_radius = lookup_radius
        self._tabu_list = deque()
        super().__init__(initial_solution, steps_threshold=steps_threshold, time_threshold=time_threshold)

    def make_step(self, state):
        neighbors = self.get_neighbors(state)
        chosen_neighbors = set(sample(neighbors, min(self.__lookup_radius, len(neighbors))))
        chosen_neighbors -= set(self._tabu_list)  # TODO: Think of a better implementation for this
        cheapest_neighbor = min(chosen_neighbors, key=lambda sol: sol.get_cost())
        self._tabu_list.append(cheapest_neighbor)
        if len(self._tabu_list) > self.__capacity:
            self._tabu_list.popleft()

        return cheapest_neighbor

    def start_search(self):
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