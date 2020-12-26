from enum import Enum


class TreePlanBuilderOrder(Enum):

    """
    The various algorithms for constructing an tree-plan topology.
    """
    LEFT_DEEP_TREE = 0,
    RIGHT_DEEP_TREE = 1,
    BALANCED_DEEP_TREE = 2

