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
    def __init__(self, execution_mode: ParallelExecutionModes, platform: ParallelExecutionPlatforms,
                 data_parallel_mode: DataParallelExecutionModes = DefaultConfig.DEFAULT_DATA_PARALLEL_ALGORITHM,
                 units_number: int = 1):
        super().__init__(execution_mode, platform)
        self.algorithm = data_parallel_mode
        self.units_number = units_number


class DataParallelExecutionParametersHirzelAlgorithm(DataParallelExecutionParameters):
    def __init__(self, execution_mode: ParallelExecutionModes, platform: ParallelExecutionPlatforms,
                 units_number: int = 1, key: str = None):
        super().__init__(execution_mode, platform, DataParallelExecutionModes.HirzelAlgorithm, units_number)
        self.divide_key = key


class DataParallelExecutionParametersRIPAlgorithm(DataParallelExecutionParameters):
    def __init__(self, execution_mode: ParallelExecutionModes, platform: ParallelExecutionPlatforms,
                 units_number: int = 1, mult:int = 3):
        super().__init__(execution_mode, platform, DataParallelExecutionModes.RIPAlgorithm, units_number)
        self.RIP_mult = mult


class DataParallelExecutionParametersHyperCubeAlgorithm(DataParallelExecutionParameters):
    def __init__(self, execution_mode: ParallelExecutionModes, platform: ParallelExecutionPlatforms,
                 units_number: int = 1, attributes_dict:dict=None):
        super().__init__(execution_mode, platform, DataParallelExecutionModes.HyperCubeAlgorithm, units_number)
        self.divde_keys_dict = attributes_dict


