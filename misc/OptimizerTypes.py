from enum import Enum


class OptimizerTypes(Enum):
    """
    The various algorithms for determine if invoke the tree builder.
    """
    TRIVIAL = 0,
    CHANGES_AWARE = 1,
    USING_INVARIANT = 2
