from abc import ABC
from parallel.data_parallel.DataParallelExecutionAlgorithm import DataParallelExecutionAlgorithm

from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import \
    EvaluationMechanismParameters
from base.DataFormatter import DataFormatter
from base.PatternMatch import *
from stream.Stream import *


class GroupByKeyParallelExecutionAlgorithm(DataParallelExecutionAlgorithm, ABC):
    """
    Implements the key-based partitioning algorithm.
    """
    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters,
                 platform, key: str):
        super().__init__(units_number, patterns, eval_mechanism_params, platform)
        self.__key = key

    def eval(self, events: InputStream, matches: OutputStream,
             data_formatter: DataFormatter):
        raise NotImplementedError()
