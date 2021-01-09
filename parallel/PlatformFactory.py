"""
This file contains the class responsible for parallel execution platform initialization.
"""
from parallel.ParallelExecutionParameters import *
from parallel.ParallelExecutionPlatforms import ParallelExecutionPlatforms
from parallel.platform.ThreadingParallelExecutionPlatform import ThreadingParallelExecutionPlatform
from parallel.ParallelExecutionAlgorithms import *

class PlatformFactory:
    """
    Creates a parallel execution platform given its specification.
    """
    @staticmethod
    def create_parallel_execution_platform(parallel_execution_params: ParallelExecutionParameters):
        if parallel_execution_params is None:
            parallel_execution_params = ParallelExecutionParameters()
        if parallel_execution_params.platform == ParallelExecutionPlatforms.THREADING:
            return ThreadingParallelExecutionPlatform()
        raise Exception("Unknown parallel execution platform: %s" % (parallel_execution_params.platform,))

    """
       Creates a threading algorithem execution platform given its specification.
    """
    @staticmethod
    def create_data_parallel_evaluation_manager(data_parallel_params: DataParallelExecutionParameters, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters, platform):
        if data_parallel_params is None:
            data_parallel_params = DataParallelExecutionParameters()
        if data_parallel_params.algorithm == DataParallelExecutionModes.ALGORITHM1:
           return Algorithm1(data_parallel_params.numThreads, patterns, eval_mechanism_params, platform)
        if data_parallel_params.algorithm == DataParallelExecutionModes.ALGORITHM2:
            return Algorithm2(data_parallel_params.numThreads, patterns, eval_mechanism_params, platform)
        if data_parallel_params.algorithm == DataParallelExecutionModes.ALGORITHM3:
            return Algorithm3(data_parallel_params.numThreads, patterns, eval_mechanism_params, platform)
        raise Exception("Unknown parallel execution Algorithm: %s" % (data_parallel_params.algorithen,))
