"""
 Data parallel HyperCube algorithms
"""
from abc import ABC
from parallel.data_parallel.DataParallelExecutionAlgorithm import DataParallelExecutionAlgorithm
import math
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import \
    EvaluationMechanismParameters
from base.DataFormatter import DataFormatter
from base.PatternMatch import *
from stream.Stream import *
from dataclasses import dataclass


def list_equal(list1, list2):
    list1.sort()
    list2.sort()
    return list1 == list2


@dataclass(frozen=True)
class EventStructure:
    name: str
    type: str
    cube_attribute: str


class HyperCubeParallelExecutionAlgorithm(DataParallelExecutionAlgorithm, ABC):
    """
    Implements the HyperCube algorithm.
    """

    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters, platform, attributes_dict: dict):
        if isinstance(patterns, list):  # TODO: check if possible multiple patterns
            if len(patterns) > 1:
                raise Exception
            pattern = patterns[0]
        else:
            pattern = patterns

        # check that the pattern match with attributes_dict
        attributes = {e.name: e.type for e in pattern.get_primitive_events()}
        if not list_equal(list(attributes.keys()), list(attributes_dict.keys())):
            raise Exception

        self.__event_types = [EventStructure(name, type, attributes_dict.get(name)) for name, type in
                              attributes.items()]
        # self.__attributes_dict = attributes_dict
        super().__init__(units_number - 1, pattern, eval_mechanism_params, platform)

    def eval(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        raise NotImplementedError()
