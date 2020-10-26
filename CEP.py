"""
This file contains the primary engine. It processes streams of events and detects pattern matches
by invoking the rest of the system components.
"""
from base.DataFormatter import DataFormatter
from stream.Stream import InputStream, OutputStream
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import (
    EvaluationMechanismParameters,
)
from typing import List
from datetime import datetime

from parallelism.EvaluationMechanismManager import EvaluationMechanismManager


class CEP:
    """
    A CEP object contains a workload (list of patterns to be evaluated) and an evaluation mechanism.
    The evaluation mechanism is created according to the parameters specified in the constructor.
    """
    def __init__(self, patterns: List[Pattern], eval_mechanism_params: EvaluationMechanismParameters = None):
        """
        Constructor of the class.
        """
        if patterns is None:
            raise Exception("No patterns are provided")
        if len(patterns) > 1:
            raise NotImplementedError("Multi-pattern support is not yet available")
        self.__eval_mechanism_manager = EvaluationMechanismManager(None, eval_mechanism_params, patterns)

    def run(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """
        Applies the evaluation mechanism to detect the predefined patterns in a given stream of events.
        Returns the total time elapsed during evaluation.
        """
        start = datetime.now()
        self.__eval_mechanism_manager.eval(events, matches, data_formatter)
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
        return self.__eval_mechanism_manager.pattern_matches_stream

    def get_evaluation_mechanism_structure_summary(self):
        """
        Returns an object summarizing the structure of the underlying evaluation mechanism.
        """
        return self.__eval_mechanism_manager.get_structure_summary()
