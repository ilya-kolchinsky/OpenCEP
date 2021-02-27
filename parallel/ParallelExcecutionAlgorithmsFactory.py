from parallel.ParallelExecutionParameters import *
from parallel.ParallelExecutionAlgorithms import *


class ParallelExecutionAlgorithmsFactory:
    """
    Creates a threading algorithm execution platform given its specification.
    """

    @staticmethod
    def create_data_parallel_evaluation(
            data_parallel_params: ParallelExecutionParameters,
            patterns: Pattern or List[Pattern],
            eval_mechanism_params: EvaluationMechanismParameters,
            platform):
        if data_parallel_params is None:
            data_parallel_params = DataParallelExecutionParametersRIPAlgorithm()
        if data_parallel_params.algorithm == DataParallelExecutionModes.HIRZEL_ALGORITHM:
            return HirzelAlgorithm(data_parallel_params.units_number,
                                   patterns, eval_mechanism_params,
                                   platform,
                                   data_parallel_params.divide_key)
        if data_parallel_params.algorithm == DataParallelExecutionModes.RIP_ALGORITHM:
            return RIPAlgorithm(data_parallel_params.units_number,
                                patterns, eval_mechanism_params, platform,
                                data_parallel_params.rip_multiple)
        if data_parallel_params.algorithm == DataParallelExecutionModes.HYPER_CUBE_ALGORITHM:
            return HyperCubeAlgorithm(data_parallel_params.units_number,
                                      patterns, eval_mechanism_params,
                                      platform,
                                      data_parallel_params.divide_keys_dict)
        raise Exception("Unknown parallel execution Algorithm: %s" % (data_parallel_params.algorithm,))
