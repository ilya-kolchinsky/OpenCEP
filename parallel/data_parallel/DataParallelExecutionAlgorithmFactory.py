from parallel.data_parallel.GroupByKeyParallelExecutionAlgorithm import GroupByKeyParallelExecutionAlgorithm
from parallel.data_parallel.HyperCubeParallelExecutionAlgorithm import HyperCubeParallelExecutionAlgorithm
from parallel.ParallelExecutionParameters import *
from parallel.data_parallel.DataParallelExecutionAlgorithm import *
from parallel.data_parallel.RIPParallelExecutionAlgorithm import RIPParallelExecutionAlgorithm
from parallel.platform.ParallelExecutionPlatform import ParallelExecutionPlatform


class DataParallelExecutionAlgorithmFactory:
    """
    Creates a parallel algorithm implementing the data-parallel paradigm.
    """
    @staticmethod
    def create_data_parallel_algorithm(data_parallel_params: DataParallelExecutionParameters,
                                       patterns: Pattern or List[Pattern],
                                       eval_mechanism_params: EvaluationMechanismParameters,
                                       platform: ParallelExecutionPlatform):
        if data_parallel_params.algorithm == DataParallelExecutionModes.GROUP_BY_KEY_ALGORITHM:
            return GroupByKeyParallelExecutionAlgorithm(data_parallel_params.units_number,
                                                        patterns, eval_mechanism_params,
                                                        platform,
                                                        data_parallel_params.divide_key)
        if data_parallel_params.algorithm == DataParallelExecutionModes.RIP_ALGORITHM:
            return RIPParallelExecutionAlgorithm(data_parallel_params.units_number,
                                                 patterns, eval_mechanism_params, platform,
                                                 data_parallel_params.rip_multiple)
        if data_parallel_params.algorithm == DataParallelExecutionModes.HYPER_CUBE_ALGORITHM:
            return HyperCubeParallelExecutionAlgorithm(data_parallel_params.units_number,
                                                       patterns, eval_mechanism_params,
                                                       platform,
                                                       data_parallel_params.divide_keys_dict)
        raise Exception("Unknown parallel execution Algorithm: %s" % (data_parallel_params.algorithm,))
