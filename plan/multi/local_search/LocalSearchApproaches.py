from enum import Enum


class LocalSearchApproaches(Enum):
    """
    The various approaches of creating a global evaluation plan with the local search algorithm.
    """
    SIMULATED_ANNEALING_SEARCH = 0
    TABU_SEARCH = 1
