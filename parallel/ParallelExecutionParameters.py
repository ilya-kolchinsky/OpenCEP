from misc import DefaultConfig
from parallel.ParallelExecutionModes import ParallelExecutionModes


class ParallelExecutionParameters:
    """
    Parameters for parallel and/or distributed execution of OpenCEP.
    """
    def __init__(self, execution_mode: ParallelExecutionModes = DefaultConfig.DEFAULT_PARALLEL_EXECUTION_MODE):
        self.execution_mode = execution_mode
