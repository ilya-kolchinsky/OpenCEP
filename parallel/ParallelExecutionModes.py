from enum import Enum


class ParallelExecutionModes(Enum):
    """
    Types of parallel processing modes supported by the system.
    """
    SEQUENTIAL = 0 # no parallelism

    # TODO: not yet implemented
    DATA_PARALLELISM = 1
    STRUCTURE_PARALLELISM = 2
    TASK_PARALLELISM = 3
    HYBRID_PARALLELISM = 4


class DataParallelExecutionModes(Enum):

    ALGORITHM1 = 1  # Hizel
    ALGORITHM2 = 2  # Rip
    ALGORITHM3 = 3  #
