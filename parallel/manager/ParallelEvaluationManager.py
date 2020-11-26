from abc import ABC

from parallel.ParallelExecutionParameters import *
from parallel.PlatformFactory import PlatformFactory
from parallel.manager.EvaluationManager import EvaluationManager

from parallel.ParallelExecutionModes import DataParallelExecutionModes
from misc.DefaultConfig import DEFAULT_DATA_PARALLEL_ALGORITHEM
from typing import List
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters

class ParallelEvaluationManager(EvaluationManager, ABC):
    """
    An abstract base class for all parallel evaluation managers.
    """
    def __init__(self, parallel_execution_params: ParallelExecutionParameters):
        self._platform = PlatformFactory.create_parallel_execution_platform(parallel_execution_params)


class DataParallelEvaluationManager(ParallelEvaluationManager):

    def __init__(self, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters,
                 parallel_execution_params: ParallelExecutionParameters,
                data_parallel_params: DataParallelExecutionParameters):

        super().__init__(parallel_execution_params)
        self.mode = data_parallel_params.algorithm
        self.numThreads = data_parallel_params.numThreads
        threads = []





