from typing import Dict

from adaptive.optimizer.Optimizer import Optimizer
from base.Pattern import Pattern
from misc import DefaultConfig
from plan.TreePlan import TreePlan
from plan.multi.local_search.LocalSearchApproaches import LocalSearchApproaches
from plan.multi.local_search.LocalSearch import TabuSearch, SimulatedAnnealingSearch


class LocalSearchParameters:
    """
    Parameters for the local search.
    """
    def __init__(self, search_type: LocalSearchApproaches = DefaultConfig.DEFAULT_SEARCH_TYPE,
                 neighborhood_vertex_size: int = DefaultConfig.NEIGHBORHOOD_VERTEX_SIZE,
                 time_limit: float = DefaultConfig.LOCAL_SEARCH_TIME_LIMIT,
                 steps_threshold: int = DefaultConfig.LOCAL_SEARCH_STEPS_THRESHOLD):
        self.search_type = search_type
        self.neighborhood_vertex_size = neighborhood_vertex_size
        self.time_limit = time_limit
        self.steps_threshold = steps_threshold


class TabuSearchLocalSearchParameters(LocalSearchParameters):
    """
    Parameters for the creation of the TabuSearch class.
    """
    def __init__(self, neighborhood_vertex_size: int = DefaultConfig.NEIGHBORHOOD_VERTEX_SIZE,
                 time_limit: float = DefaultConfig.LOCAL_SEARCH_TIME_LIMIT,
                 steps_threshold: int = DefaultConfig.LOCAL_SEARCH_STEPS_THRESHOLD,
                 capacity: int = DefaultConfig.TABU_SEARCH_CAPACITY,
                 neighborhood_size: int = DefaultConfig.TABU_SEARCH_NEIGHBORHOOD_SIZE):
        self.capacity = capacity
        self.neighborhood_size = neighborhood_size
        super().__init__(LocalSearchApproaches.TABU_SEARCH, neighborhood_vertex_size,
                         time_limit, steps_threshold)


class SimulatedAnnealingLocalSearchParameters(LocalSearchParameters):
    """
    Parameters for the creation of the SimulatedAnnealingSearch class.
    """
    def __init__(self, neighborhood_vertex_size: int = DefaultConfig.NEIGHBORHOOD_VERTEX_SIZE,
                 time_limit: float = DefaultConfig.LOCAL_SEARCH_TIME_LIMIT,
                 steps_threshold: int = DefaultConfig.LOCAL_SEARCH_STEPS_THRESHOLD,
                 initial_neighbors: int = DefaultConfig.SIMULATED_ANNEALING_INIT_NEIGHBORS,
                 multiplier: float = DefaultConfig.SIMULATED_ANNEALING_MULTIPLIER,
                 simulated_annealing_threshold: float = DefaultConfig.SIMULATED_ANNEALING_C_THRESHOLD):
        self.initial_neighbors = initial_neighbors
        self.multiplier = multiplier
        self.simulated_annealing_threshold = simulated_annealing_threshold
        super().__init__(LocalSearchApproaches.SIMULATED_ANNEALING_SEARCH, neighborhood_vertex_size,
                         time_limit, steps_threshold)


class LocalSearchFactory:
    """
    Creates a local search object given its specification.
    """
    @staticmethod
    def build_local_search(pattern_to_tree_plan_map: Dict[Pattern, TreePlan], optimizer: Optimizer,
                           local_search_parameters: LocalSearchParameters):
        if local_search_parameters is None:
            local_search_parameters = LocalSearchFactory.__create_default_local_search_parameters()
        return LocalSearchFactory.__create_local_search(pattern_to_tree_plan_map, optimizer, local_search_parameters)

    @staticmethod
    def __create_local_search(pattern_to_tree_plan_map: Dict[Pattern, TreePlan], optimizer: Optimizer,
                              local_search_parameters):
        mutual_params = [pattern_to_tree_plan_map, optimizer, local_search_parameters.steps_threshold,
                         local_search_parameters.time_limit, local_search_parameters.neighborhood_vertex_size]

        if local_search_parameters.search_type == LocalSearchApproaches.TABU_SEARCH:
            return TabuSearch(*mutual_params, local_search_parameters.capacity,
                              local_search_parameters.neighborhood_size)

        if local_search_parameters.search_type == LocalSearchApproaches.SIMULATED_ANNEALING_SEARCH:
            return SimulatedAnnealingSearch(*mutual_params, local_search_parameters.multiplier,
                                            local_search_parameters.simulated_annealing_threshold,
                                            local_search_parameters.initial_neighbors)

        raise Exception("Unknown local search type specified")

    @staticmethod
    def __create_default_local_search_parameters():
        """
        Uses default configurations to create local search parameters.
        """
        if DefaultConfig.DEFAULT_SEARCH_TYPE == LocalSearchApproaches.TABU_SEARCH:
            return TabuSearchLocalSearchParameters()
        if DefaultConfig.DEFAULT_SEARCH_TYPE == LocalSearchApproaches.SIMULATED_ANNEALING_SEARCH:
            return SimulatedAnnealingLocalSearchParameters()
        raise Exception("Unknown local search type: %s" % (DefaultConfig.DEFAULT_SEARCH_TYPE,))

