"""
This file contains the primary engine. It processes streams of events and detects pattern matches
by invoking the rest of the system components.
"""
from misc.IOUtils import Stream
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters, \
    EvaluationMechanismTypes, EvaluationMechanismFactory
from typing import List
from datetime import datetime
from statisticsCollector.StatisticsCollector import StatisticsCollector
from optimizer.Optimizer import Optimizer
from evaluation.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism


class PerformanceSpecifications:
    """
    A sketch of QoS specifications, we assume it will be an object constructed separately, and the
    CEP engine will refer to it if it is passed.
    Not implemented yet.
    """
    pass


class CEP:
    """
    A CEP object contains a workload (list of patterns to be evaluated) and an evaluation mechanism.
    The evaluation mechanism is created according to the parameters specified in the constructor.
    """
    def __init__(self, patterns: List[Pattern],
                 eval_mechanism_type: EvaluationMechanismTypes = EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE,
                 eval_mechanism_params: EvaluationMechanismParameters = None,
                 performance_specs: PerformanceSpecifications = None
                 ):
        """
        Constructor of the class
        """
        if patterns is None:
            raise Exception("No patterns are provided")
        if len(patterns) > 1:
            raise NotImplementedError("Multi-pattern support is not yet available")
        self.eval_mechanism_params = eval_mechanism_params
        self.adaptive_parameters = None if eval_mechanism_params is None else eval_mechanism_params.adaptive_parameters
        self.__statistics_collector = None
        self.__optimizer = None
        if self.adaptive_parameters is not None:
            self.__statistics_collector = StatisticsCollector(patterns[0], self.adaptive_parameters.statistics_type,
                                                              self.adaptive_parameters.data_manager_type)
            self.__optimizer = Optimizer(patterns[0], eval_mechanism_type, eval_mechanism_params)
            _, initial_order_for_tree = EvaluationMechanismFactory.build_single_pattern_eval_mechanism(
                                                                        EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE,
                                                                        self.eval_mechanism_params, patterns[0])
            self.__eval_mechanism = TreeBasedEvaluationMechanism(patterns[0], initial_order_for_tree,
                                                                 self.__statistics_collector)
        else:
            self.__eval_mechanism, _ = EvaluationMechanismFactory.build_single_pattern_eval_mechanism(eval_mechanism_type,
                                                                                                   self.eval_mechanism_params,
                                                                                                   patterns[0])
        self.__pattern_matches = None
        self.__performance_specs = performance_specs

    def run(self, event_stream: Stream):
        """
        Applies the evaluation mechanism to detect the predefined patterns in a given stream of events.
        Returns the total time elapsed during evaluation.
        """
        self.__pattern_matches = Stream()
        start = datetime.now()
        self.__eval_mechanism.eval(events=event_stream, matches=self.__pattern_matches,
                                   statistics_collector=self.__statistics_collector, optimizer=self.__optimizer,
                                   eval_mechanism_params=self.eval_mechanism_params)
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

    # For future support of dynamic workload modification
    def add_pattern(self, pattern: Pattern, priority: int = 0):
        raise NotImplementedError()

    # For future support of dynamic workload modification
    def remove_pattern(self, pattern: Pattern, priority: int = 0):
        raise NotImplementedError()
