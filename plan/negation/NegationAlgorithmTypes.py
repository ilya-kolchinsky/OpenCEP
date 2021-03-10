from enum import Enum


class NegationAlgorithmTypes(Enum):
    """
    The various negation algorithms.
    """
    NAIVE_NEGATION_ALGORITHM = 0,
    STATISTIC_NEGATION_ALGORITHM = 1,
    LOWEST_POSITION_NEGATION_ALGORITHM = 2  # not yet supported
