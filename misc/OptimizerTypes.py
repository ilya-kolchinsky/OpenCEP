from enum import Enum


class OptimizerTypes(Enum):
    """
    The various algorithms for determine if invoke the tree builder.
    """
    TRIVIAL = 0,
    STATISTICS_DEVIATION_AWARE = 1,
    USING_INVARIANT = 2
