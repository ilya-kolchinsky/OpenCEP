import random
from enum import Enum


class IterativeImprovementType(Enum):
    """
    Two types of iterative improvement moves are supported:
    - swap (select two events and swap their locations in the current order)
    - circle (select three events and cycle their locations in the current order)
    """
    SWAP_BASED = 0
    CIRCLE_BASED = 1


class IterativeImprovementInitType(Enum):
    """
    The way of initializing the initial state for plan generation.
    """
    RANDOM = 0
    GREEDY = 1


class IterativeImprovement:
    """
    Implements the generic iterative improvement algorithm.
    """
    def execute(self, step_limit: int, initial_order: list, get_cost_callback: callable):
        new_order = initial_order.copy()
        curr_cost = get_cost_callback(new_order)
        for step in range(step_limit):
            current_move = self._movement_generator(len(new_order))
            self._movement_function(new_order, current_move)
            new_cost = get_cost_callback(new_order)
            if new_cost < curr_cost:
                curr_cost = new_cost
            else:
                self._movement_function(new_order, self._reverse_move(current_move))
        return new_order

    def _movement_generator(self, movement_range: int):
        raise NotImplementedError()

    def _movement_function(self, order: list, move: object):
        raise NotImplementedError()

    def _reverse_move(self, move: object):
        raise NotImplementedError()


class SwapBasedIterativeImprovement(IterativeImprovement):
    """
    Implements the swap-based iterative improvement algorithm.
    """
    def _movement_generator(self, movement_range: int):
        i = random.randint(0, movement_range - 1)
        j = random.randint(i, movement_range - 1)
        return i, j

    def _movement_function(self, order: list, move: object):
        i, j = move
        temp = order[i]
        order[i] = order[j]
        order[j] = temp

    def _reverse_move(self, move: object):
        i, j = move
        return j, i


class CircleBasedIterativeImprovement(IterativeImprovement):
    """
    Implements the circle-based iterative improvement algorithm.
    """
    def _movement_generator(self, movement_range: int):
        i = random.randint(0, movement_range - 3)
        j = random.randint(i + 1, movement_range - 2)
        k = random.randint(j + 1, movement_range - 1)
        if random.randint(0, 1) == 1:
            return i, j, k
        return i, k, j

    def _movement_function(self, order: list, move: object):
        i, j, k = move
        tmp = order[i]
        order[i] = order[j]
        order[j] = order[k]
        order[k] = tmp

    def _reverse_move(self, move: object):
        i, j, k = move
        return k, i, j


class IterativeImprovementAlgorithmBuilder:
    """
    A class for creating an iterative improvement algorithm according to the specified type.
    """
    @staticmethod
    def create_ii_algorithm(ii_type: IterativeImprovementType = IterativeImprovementType.SWAP_BASED):
        if ii_type == IterativeImprovementType.SWAP_BASED:
            return SwapBasedIterativeImprovement()
        elif ii_type == IterativeImprovementType.CIRCLE_BASED:
            return CircleBasedIterativeImprovement()
        return None
