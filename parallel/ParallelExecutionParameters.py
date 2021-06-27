from misc import DefaultConfig
from parallel.ParallelExecutionModes import *
from parallel.ParallelExecutionPlatforms import ParallelExecutionPlatforms
from datetime import timedelta


class ParallelExecutionParameters:
    """
    Parameters for parallel and/or distributed execution of OpenCEP.
    """

    def __init__(self,
                 execution_mode: ParallelExecutionModes = DefaultConfig.DEFAULT_PARALLEL_EXECUTION_MODE,
                 platform: ParallelExecutionPlatforms = DefaultConfig.DEFAULT_PARALLEL_EXECUTION_PLATFORM):
        self.execution_mode = execution_mode
        self.platform = platform


class DataParallelExecutionParameters(ParallelExecutionParameters):
    """
        Parameters for data parallel algorithms.
    """
    def __init__(self,
                 execution_mode: ParallelExecutionModes = ParallelExecutionModes.DATA_PARALLELISM,
                 platform: ParallelExecutionPlatforms = DefaultConfig.DEFAULT_PARALLEL_EXECUTION_PLATFORM,
                 data_parallel_mode: DataParallelExecutionModes = DefaultConfig.DEFAULT_DATA_PARALLEL_ALGORITHM,
                 units_number: int = DefaultConfig.DEFAULT_PARALLEL_UNITS_NUMBER, debug: bool = False):
        super().__init__(execution_mode, platform)
        self.algorithm = data_parallel_mode
        self.units_number = units_number
        self.debug = debug


class DataParallelExecutionParametersHirzelAlgorithm(DataParallelExecutionParameters):
    """
        Parameters for Hirzel algorithm.
    """
    def __init__(self,
                 execution_mode: ParallelExecutionModes = ParallelExecutionModes.DATA_PARALLELISM,
                 platform: ParallelExecutionPlatforms = DefaultConfig.DEFAULT_PARALLEL_EXECUTION_PLATFORM,
                 units_number: int = DefaultConfig.DEFAULT_PARALLEL_UNITS_NUMBER,
                 key: str = DefaultConfig.DEFAULT_PARALLEL_KEY, debug: bool = False):
        super().__init__(execution_mode, platform,
                         DataParallelExecutionModes.GROUP_BY_KEY_ALGORITHM,
                         units_number, debug)
        self.divide_key = key


class DataParallelExecutionParametersRIPAlgorithm(DataParallelExecutionParameters):
    """
            Parameters for RIP algorithm.
    """
    def __init__(self,
                 execution_mode: ParallelExecutionModes = ParallelExecutionModes.DATA_PARALLELISM,
                 platform: ParallelExecutionPlatforms = DefaultConfig.DEFAULT_PARALLEL_EXECUTION_PLATFORM,
                 units_number: int = DefaultConfig.DEFAULT_PARALLEL_UNITS_NUMBER,
                 interval: timedelta = DefaultConfig.DEFAULT_PARALLEL_INTERVAL, debug: bool = False):
        super().__init__(execution_mode, platform,
                         DataParallelExecutionModes.RIP_ALGORITHM,
                         units_number, debug)
        self.rip_interval = interval


class DataParallelExecutionParametersHyperCubeAlgorithm(DataParallelExecutionParameters):
    """
            Parameters for HyperCube algorithm
    """
    def __init__(self,
                 execution_mode: ParallelExecutionModes = ParallelExecutionModes.DATA_PARALLELISM,
                 platform: ParallelExecutionPlatforms = DefaultConfig.DEFAULT_PARALLEL_EXECUTION_PLATFORM,
                 units_number: int = DefaultConfig.DEFAULT_PARALLEL_UNITS_NUMBER,
                 attributes_dict: dict = DefaultConfig.DEFAULT_PARALLEL_ATTRIBUTES_DICT, debug: bool = False):
        super().__init__(execution_mode, platform,
                         DataParallelExecutionModes.HYPER_CUBE_ALGORITHM,
                         units_number, debug)
        self.divide_keys_dict = attributes_dict
