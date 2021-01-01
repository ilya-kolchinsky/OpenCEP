from enum import Enum


class TreePlanBuilderOrder(Enum):

    """
    The various algorithms for constructing an tree-plan topology.
    """
    LEFT_TREE = 0,
    RIGHT_TREE = 1,
    BALANCED_TREE = 2,
    HALF_LEFT_HALF_BALANCED_TREE = 2


