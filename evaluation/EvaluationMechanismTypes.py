from enum import Enum


class EvaluationMechanismTypes(Enum):
    """
    The various algorithms for constructing an efficient evaluation tree.
    """
    TRIVIAL_LEFT_DEEP_TREE = 0,
    SORT_BY_FREQUENCY_LEFT_DEEP_TREE = 1,
    GREEDY_LEFT_DEEP_TREE = 2,
    LOCAL_SEARCH_LEFT_DEEP_TREE = 3,
    DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE = 4,
    DYNAMIC_PROGRAMMING_BUSHY_TREE = 5,
    ZSTREAM_BUSHY_TREE = 6,
    ORDERED_ZSTREAM_BUSHY_TREE = 7