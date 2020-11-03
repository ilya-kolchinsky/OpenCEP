from misc import DefaultConfig
from parallel.ParallelExecutionModes import ParallelExecutionModes
from parallel.ParallelExecutionPlatforms import ParallelExecutionPlatforms


class ParallelExecutionParameters:
    """
    Parameters for parallel and/or distributed execution of OpenCEP.
    """
    def __init__(self,
                 execution_mode: ParallelExecutionModes = DefaultConfig.DEFAULT_PARALLEL_EXECUTION_MODE,
                 platform: ParallelExecutionPlatforms = DefaultConfig.DEFAULT_PARALLEL_EXECUTION_PLATFORM):
        self.execution_mode = execution_mode
        self.platform = platform
