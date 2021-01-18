"""
This file contains an implementation of the "trivial" evaluation manager with no parallelization capabilities.
"""
from evaluation.EvaluationMechanismFactory import (
    EvaluationMechanismParameters,
    EvaluationMechanismFactory,
)
from optimizer.OptimizerFactory import OptimizerFactory, OptimizerParameters
from statistics_collector import NewStatisticsFactory
from statistics_collector.statisticsCollectorFactory import (
    StatisticsCollectorParameters,

)
from parallel.manager.EvaluationManager import EvaluationManager
from stream.Stream import InputStream, OutputStream
from base.Pattern import Pattern
from base.DataFormatter import DataFormatter
from statistics_collector.NewStatCollectorFactory import StatCollectorFactory, StatCollectorParameters
from typing import List


class SequentialEvaluationManager(EvaluationManager):
    """
    A trivial implementation of an evaluation manager with no parallelization capabilities.
    Initializes a single evaluation mechanism and delegates to it the entire workload.
    """
    def __init__(self, patterns: Pattern or List[Pattern], eval_mechanism_params: EvaluationMechanismParameters,
                 statistics_collector_params: StatCollectorParameters, optimizer_parameters: OptimizerParameters):
        if isinstance(patterns, Pattern):
            patterns = [patterns]
        if len(patterns) > 1:
            self.__eval_mechanism = EvaluationMechanismFactory.build_multi_pattern_eval_mechanism(eval_mechanism_params,
                                                                                                  patterns)
        else:
            statistics_collector = StatCollectorFactory.build_statistics_collector(statistics_collector_params, patterns)
            optimizer = OptimizerFactory.build_optimizer(optimizer_parameters)
            self.__eval_mechanism = EvaluationMechanismFactory.build_single_pattern_eval_mechanism(eval_mechanism_params,
                                                                                                   patterns[0],
                                                                                                   statistics_collector,
                                                                                                   optimizer)
        self.__pattern_matches = None

    def eval(self, event_stream: InputStream, pattern_matches: OutputStream, data_formatter: DataFormatter):
        self.__pattern_matches = pattern_matches
        self.__eval_mechanism.eval(event_stream, pattern_matches, data_formatter)

    def get_pattern_match_stream(self):
        return self.__pattern_matches

    def get_structure_summary(self):
        return self.__eval_mechanism.get_structure_summary()
