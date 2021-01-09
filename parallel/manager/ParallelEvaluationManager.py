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
from stream.DataParallelStream import *
from base.DataFormatter import DataFormatter
from stream.Stream import Stream

import threading


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
        self.__mode = data_parallel_params.algorithm
        self.__numThreads = data_parallel_params.numThreads
        self.__algorithm = PlatformFactory.create_data_parallel_evaluation_manager(data_parallel_params, patterns, eval_mechanism_params, self._platform)
        self.__pattern_matches = None

    def eval(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):

        self.__pattern_matches = matches
        self.__algorithm.eval_algorithm(events, matches, data_formatter)# for now it copy all the output stream to the match stream inside the algorithms's classes


    def get_pattern_match_stream(self):
        return self.__pattern_matches

"""
    def get_structure_summary(self):
        return self.__eval_mechanism.get_structure_summary()
"""






