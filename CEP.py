"""
This file contains the primary engine. It processes streams of events and detects pattern matches
by invoking the rest of the system components.
"""
from base.DataFormatter import DataFormatter
from stream.Stream import InputStream, OutputStream
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import (
    EvaluationMechanismParameters,
    EvaluationMechanismFactory,
)
from typing import List
from datetime import datetime


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
        self.__eval_mechanism = EvaluationMechanismFactory.build_single_pattern_eval_mechanism(eval_mechanism_params,
                                                                                               patterns[0])
        self.__pattern_matches = None

    def run(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """
        Applies the evaluation mechanism to detect the predefined patterns in a given stream of events.
        Returns the total time elapsed during evaluation.
        """
        self.__pattern_matches = matches
        start = datetime.now()
        self.__eval_mechanism.eval(events, self.__pattern_matches, data_formatter)
        return (datetime.now() - start).total_seconds()

    def get_pattern_match(self):
        """
        Returns one match from the output stream.
        """
        if self.__pattern_matches is None:
            return None
        try:
            return self.__pattern_matches.get_item()
        except StopIteration:  # the stream might be closed.
            return None

    def get_pattern_match_stream(self):
        """
        Returns the output stream containing the detected matches.
        """
        return self.__pattern_matches

    def get_evaluation_mechanism_structure_summary(self):
        """
        Returns an object summarizing the structure of the underlying evaluation mechanism.
        """
        return self.__eval_mechanism.get_structure_summary()
