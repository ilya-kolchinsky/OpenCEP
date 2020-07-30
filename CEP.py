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
from optimizer.ReoptimizingDecision import ReoptimizationDecisionTypes
from statisticsCollector.StatisticsCollector import StatisticsCollector, Stat
from statisticsCollector.StatisticsTypes import StatisticsTypes
from optimizer.Optimizer import Optimizer
from AdaptiveConfigurationSettings import AdaptiveParameters
from evaluation.AdaptiveTreeReplacementAlgorithm import StopAndReplaceTree,\
    create_adaptive_evaluation_mechanism_by_type
import time


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
                 reoptimizing_decision_params: ReoptimizationDecisionTypes,
                 eval_mechanism_type: EvaluationMechanismTypes = EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE,
                 eval_mechanism_params: EvaluationMechanismParameters = None,
                 performance_specs: PerformanceSpecifications = None,
                 is_adaptive=False, adaptive_parameters = None
                 ):
        """
        Constructor of the class.
        reoptimizing_decision_params will include type and extra paratmeters if needed
        for example it will have (ReoptimizationDecisionTypes.STATIC_THRESHOLD_BASED, threshold)
        """
        if patterns is None:
            raise Exception("No patterns are provided")
        if len(patterns) > 1:
            raise NotImplementedError("Multi-pattern support is not yet available")
        if is_adaptive:
            self.adaptive_parameters = adaptive_parameters
            self.__statistics_collector = StatisticsCollector(patterns[0], adaptive_parameters.statistics_type,
                                                              adaptive_parameters.window_coefficient,
                                                              adaptive_parameters.k)
            self.__optimizer = Optimizer(patterns[0], eval_mechanism_type, adaptive_parameters.statistics_type,
                                         adaptive_parameters.reoptimizing_decision_params)
            initial_tree = EvaluationMechanismFactory.build_adaptive_single_pattern_eval_mechanism(
                                                                        EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE,
                                                                        None, patterns[0], None, False)
            self.__eval_mechanism = create_adaptive_evaluation_mechanism_by_type(adaptive_parameters.tree_replacement_algorithm_type,
                                                                                 patterns[0], initial_tree)
        else:
            self.__eval_mechanism = EvaluationMechanismFactory.build_single_pattern_eval_mechanism(eval_mechanism_type,
                                                                                               eval_mechanism_params,
                                                                                               patterns[0])
        self.is_adaptive = is_adaptive
        self.__pattern_matches = None
        self.__performance_specs = performance_specs

    def run(self, event_stream: Stream):
        """
        Applies the evaluation mechanism to detect the predefined patterns in a given stream of events.
        Returns the total time elapsed during evaluation.
        """
        self.__pattern_matches = Stream()
        start = datetime.now()
        last_activated_statistics_collector_period = last_activated_optimizer_period = time.time()
        stat = self.__statistics_collector.get_stat()
        if self.is_adaptive:
            for event in event_stream:
                if time.time() - last_activated_statistics_collector_period >\
                        self.adaptive_parameters.activate_statistics_collector_period:
                    self.__statistics_collector.insert_event(event)
                    stat = self.__statistics_collector.get_stat()
                    last_activated_statistics_collector_period = time.time()
                if time.time() - last_activated_optimizer_period >\
                        self.adaptive_parameters.activate_optimizer_period:
                    new_plan = self.__optimizer.run(stat)
                    last_activated_optimizer_period = time.time()
                else:
                    new_plan = None
                self.__eval_mechanism.eval(event, new_plan, self.__pattern_matches, self.__statistics_collector)
            # After the loop
            self.__pattern_matches.close()  # TODO: IMPORTANT!!
        else:
            self.__eval_mechanism.eval(event_stream, self.__pattern_matches)
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
