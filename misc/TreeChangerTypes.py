from enum import Enum


class TreeChangerTypes(Enum):
    """
    The various algorithms for determine how to change tree in run time.
    """
    TRIVIAL_TREE_CHANGER = 0
    PARALLEL_TREE_CHANGER = 1
