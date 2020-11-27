"""
This file contains the main class of the project. It processes streams of events and detects pattern matches
by invoking the rest of the system components.
"""
from base.DataFormatter import DataFormatter
from parallel.EvaluationManagerFactory import EvaluationManagerFactory
from parallel.ParallelExecutionParameters import ParallelExecutionParameters
from stream.Stream import InputStream, OutputStream
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from typing import List
from datetime import datetime


class CEP:
    """
    A CEP object wraps the engine responsible for actual processing. It accepts the desired workload (list of patterns
    to be evaluated) and a set of settings defining the evaluation mechanism to be used and the way the workload should
    be optimized and parallelized.
    """
    def __init__(self, patterns: Pattern or List[Pattern], eval_mechanism_params: EvaluationMechanismParameters = None,
                 parallel_execution_params: ParallelExecutionParameters = None):
        """
        Constructor of the class.
        """
        if patterns is None or len(patterns) == 0:
            raise Exception("No patterns are provided")
        self.__evaluation_manager = EvaluationManagerFactory.create_evaluation_manager(patterns,
                                                                                       eval_mechanism_params,
                                                                                       parallel_execution_params)

    def run(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """
        Applies the evaluation mechanism to detect the predefined patterns in a given stream of events.
        Returns the total time elapsed during evaluation.
        """
        start = datetime.now()
        self.__evaluation_manager.eval(events, matches, data_formatter)
        return (datetime.now() - start).total_seconds()

    def get_pattern_match(self):
        """
        Returns one match from the output stream.
        """
        try:
            return self.get_pattern_match_stream().get_item()
        except StopIteration:  # the stream might be closed.
            return None

    def get_pattern_match_stream(self):
        """
        Returns the output stream containing the detected matches.
        """
        return self.__evaluation_manager.get_pattern_match_stream()

    def get_evaluation_mechanism_structure_summary(self):
        """
        Returns an object summarizing the structure of the underlying evaluation mechanism.
        """
        return self.__evaluation_manager.get_structure_summary()
