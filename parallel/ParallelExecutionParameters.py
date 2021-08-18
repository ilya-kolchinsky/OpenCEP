from misc import DefaultConfig
from parallel.ParallelExecutionModes import *
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


class DataParallelExecutionParameters(ParallelExecutionParameters):
    """
        Parameters for data parallel algorithms.
    """
    def __init__(self,
                 platform: ParallelExecutionPlatforms = DefaultConfig.DEFAULT_PARALLEL_EXECUTION_PLATFORM,
                 data_parallel_mode: DataParallelExecutionModes = DefaultConfig.DEFAULT_DATA_PARALLEL_ALGORITHM,
                 units_number: int = DefaultConfig.DEFAULT_PARALLEL_UNITS_NUMBER):
        if units_number <= 0:
            raise Exception(f"units_number must be positive number, got {units_number}")
        super().__init__(execution_mode=ParallelExecutionModes.DATA_PARALLELISM, platform=platform)
        self.algorithm = data_parallel_mode
        self.units_number = units_number


class DataParallelExecutionParametersHirzelAlgorithm(DataParallelExecutionParameters):
    """
        Parameters for Hirzel algorithm.
    """
    def __init__(self,
                 platform: ParallelExecutionPlatforms = DefaultConfig.DEFAULT_PARALLEL_EXECUTION_PLATFORM,
                 units_number: int = DefaultConfig.DEFAULT_PARALLEL_UNITS_NUMBER,
                 key: str = DefaultConfig.DEFAULT_PARALLEL_KEY):
        super().__init__(platform,
                         DataParallelExecutionModes.GROUP_BY_KEY_ALGORITHM,
                         units_number)
        self.divide_key = key


class DataParallelExecutionParametersRIPAlgorithm(DataParallelExecutionParameters):
    """
            Parameters for RIP algorithm.
    """
    def __init__(self,
                 platform: ParallelExecutionPlatforms = DefaultConfig.DEFAULT_PARALLEL_EXECUTION_PLATFORM,
                 units_number: int = DefaultConfig.DEFAULT_PARALLEL_UNITS_NUMBER,
                 multiple: float = DefaultConfig.DEFAULT_PARALLEL_MULTIPLE):
        super().__init__(platform,
                         DataParallelExecutionModes.RIP_ALGORITHM,
                         units_number)
        self.rip_multiple = multiple


class DataParallelExecutionParametersHyperCubeAlgorithm(DataParallelExecutionParameters):
    """
            Parameters for HyperCube algorithm
    """
    def __init__(self,
                 platform: ParallelExecutionPlatforms = DefaultConfig.DEFAULT_PARALLEL_EXECUTION_PLATFORM,
                 units_number: int = DefaultConfig.DEFAULT_PARALLEL_UNITS_NUMBER,
                 attributes_dict: dict = DefaultConfig.DEFAULT_PARALLEL_ATTRIBUTES_DICT):
        super().__init__(platform,
                         DataParallelExecutionModes.HYPER_CUBE_ALGORITHM,
                         units_number)
        self.divide_keys_dict = attributes_dict
