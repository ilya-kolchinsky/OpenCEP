from parallel.data_parallel.DataParallelExecutionAlgorithm import DataParallelExecutionAlgorithm
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from base.DataFormatter import DataFormatter
from base.PatternMatch import *
from stream.Stream import *
from parallel.platform.ParallelExecutionPlatform import ParallelExecutionPlatform
from misc.Utils import is_int, is_float


class GroupByKeyParallelExecutionAlgorithm(DataParallelExecutionAlgorithm):
    """
    Implements the key-based partitioning algorithm.
    """

    def __init__(self,
                 units_number,
                 patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters,
                 platform: ParallelExecutionPlatform,
                 key: str):
        super().__init__(units_number, patterns, eval_mechanism_params, platform)
        self.__key = key

    def _get_event_key_value(self, raw_event, data_formatter: DataFormatter):
        payload = data_formatter.parse_event(raw_event)
        value = payload[self.__key]
        return value

    def _eval_preprocess(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        first_raw_event = events.first()
        value = self._get_event_key_value(first_raw_event, data_formatter)
        if not is_int(value) and not is_float(value):
            raise Exception('Non numeric key')

    def _classifier(self, raw_event: str, data_formatter: DataFormatter):
        value = self._get_event_key_value(raw_event, data_formatter)
        return [int(value) % self.units_number]



