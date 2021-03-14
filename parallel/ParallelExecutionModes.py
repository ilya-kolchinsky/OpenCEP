from enum import Enum


class ParallelExecutionModes(Enum):
    """
    Types of parallel processing modes supported by the system.
    """
    SEQUENTIAL = 0  # no parallelism

    # TODO: not yet implemented
    DATA_PARALLELISM = 1
    STRUCTURE_PARALLELISM = 2
    TASK_PARALLELISM = 3
    HYBRID_PARALLELISM = 4


class DataParallelExecutionModes(Enum):
    """
    Types of data parallel algorithm modes supported by the system.
    """
    GROUP_BY_KEY_ALGORITHM = 1
    RIP_ALGORITHM = 2
    HYPER_CUBE_ALGORITHM = 3
