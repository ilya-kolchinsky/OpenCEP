from enum import Enum


class TreePlanBuilderTypes(Enum):
    """
    The various algorithms for constructing an efficient tree-based pattern evaluation plan.
    """
    TRIVIAL_LEFT_DEEP_TREE = 0,
    SORT_BY_FREQUENCY_LEFT_DEEP_TREE = 1,
    GREEDY_LEFT_DEEP_TREE = 2,
    LOCAL_SEARCH_LEFT_DEEP_TREE = 3,
    DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE = 4,
    DYNAMIC_PROGRAMMING_BUSHY_TREE = 5,
    ZSTREAM_BUSHY_TREE = 6,
    ORDERED_ZSTREAM_BUSHY_TREE = 7,
    INVARIANT_AWARE_GREEDY_LEFT_DEEP_TREE = 8,  # can only be used in conjunction with the invariant-based optimizer
    INVARIANT_AWARE_ZSTREAM_BUSHY_TREE = 9  # can only be used in conjunction with the invariant-based optimizer
