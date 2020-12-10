from abc import ABC

from parallel.ParallelExecutionParameters import *
from parallel.PlatformFactory import PlatformFactory
from parallel.manager.EvaluationManager import EvaluationManager

from parallel.ParallelExecutionModes import DataParallelExecutionModes
from misc.DefaultConfig import DEFAULT_DATA_PARALLEL_ALGORITHM
from typing import List
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters, EvaluationMechanismFactory

from stream.Stream import InputStream, OutputStream
from base.DataFormatter import DataFormatter

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
        #self.patteren_matcht
        self.trees = []
        for i in range(0, self.numThreads):
            if isinstance(patterns, Pattern):
                patterns = [patterns]
            if len(patterns) > 1:
                self.trees.append(EvaluationMechanismFactory.build_multi_pattern_eval_mechanism(
                    eval_mechanism_params,
                    patterns))
            else:
                self.trees.append(EvaluationMechanismFactory.build_single_pattern_eval_mechanism(
                    eval_mechanism_params,
                    patterns[0]))
            self.__pattern_matches = None

    def eval(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):

        for tree in self.trees:
            tree.eval(events, matches, data_formatter)

        # remove duplicated










