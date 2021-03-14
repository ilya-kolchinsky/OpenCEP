from abc import ABC
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import \
    EvaluationMechanismParameters, EvaluationMechanismFactory
from base.DataFormatter import DataFormatter
from base.PatternMatch import *
from parallel.platform.ParallelExecutionPlatform import ParallelExecutionPlatform
from stream.Stream import *


class DataParallelExecutionAlgorithm(ABC):
    """
    An abstract base class for all data parallel evaluation algorithms.
    """
    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters, platform: ParallelExecutionPlatform):
        raise NotImplementedError()

    def eval(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """
        Activates the actual parallel algorithm.
        """
        raise NotImplementedError()

    def get_structure_summary(self):
        raise NotImplementedError()
