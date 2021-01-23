from enum import Enum


class MultiPatternUnifiedTreeLocalSearchApproaches(Enum):
    """
    The various approaches for neighbor function used in local search algorithm.
    """
    EDGE_NEIGHBOR = 0,
    VERTEX_NEIGHBOR = 1,
