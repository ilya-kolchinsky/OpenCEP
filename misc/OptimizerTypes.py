from enum import Enum


class OptimizerTypes(Enum):
    """
    The various optimizer types that invoke the tree builder
    """
    TRIVIAL_OPTIMIZER = 0,
    STATISTICS_DEVIATION_AWARE_OPTIMIZER = 1,
    INVARIANT_AWARE_OPTIMIZER = 2
