from enum import Enum


class ParallelExecutionPlatforms(Enum):
    """
    Supported platforms for parallel and/or distributed execution.
    """
    THREADING = 0

    # TODO: should support more types
