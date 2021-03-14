from parallel.data_parallel.DataParallelExecutionAlgorithmFactory import \
    DataParallelExecutionAlgorithmFactory, ABC
from parallel.manager.ParallelEvaluationManager import ParallelEvaluationManager
from typing import List
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from base.DataFormatter import DataFormatter
from parallel.ParallelExecutionParameters import *
from stream.Stream import *


class DataParallelEvaluationManager(ParallelEvaluationManager, ABC):
    """
    A parallel evaluation manager employing the data-parallel paradigm.
    """
    def __init__(self, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters,
                 parallel_execution_params: DataParallelExecutionParameters):
        super().__init__(parallel_execution_params)
        self.__mode = parallel_execution_params.algorithm
        self.__num_units = parallel_execution_params.units_number
        self.__algorithm = \
            DataParallelExecutionAlgorithmFactory.create_data_parallel_algorithm(
                parallel_execution_params, patterns, eval_mechanism_params, self._platform)
        self.__pattern_matches = None

    def eval(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        self.__pattern_matches = matches
        self.__algorithm.eval(events, matches, data_formatter)
        # for now it copies all the output stream to the match stream inside the algorithms classes

    def get_pattern_match_stream(self):
        return self.__pattern_matches

    def get_structure_summary(self):
        return self.__algorithm.get_structure_summary()
