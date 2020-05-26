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
                 performance_specs: PerformanceSpecifications = None):#EVA_rajouter une possibilité de choisir le NegationMode
        """
        Constructor of the class.
        """
        if patterns is None:
            raise Exception("No patterns are provided")
        if len(patterns) > 1:
            raise NotImplementedError("Multi-pattern support is not yet available")

        #EVA ici séparer patterns[0] en un pattern avec tous les Positive events et un avec les negatifs.
        #envoyer à la fonction juste les positifs

        #test = patterns[0].structure.get_args()[0].name
        NegationPostProcessing = True
        #if NegationPostProcessing:
            #PositifPattern = Pattern(patterns[0].positive_event, patterns[0].condition, patterns[0].window)

        self.__eval_mechanism = EvaluationMechanismFactory.build_single_pattern_eval_mechanism(eval_mechanism_type,
                                                                                               eval_mechanism_params,
                                                                                               patterns[0])
        #Ici rajouter sur l'arbre créé les NegationNode pour Post-Processing Mode avec la Formula dans le InternalNode
        #ensuite faire self.evalmechanism.apply_formula(pattern.neg_condition)
        #Pour le First-Chance Negation ???

        self.__pattern_matches = None
        self.__performance_specs = performance_specs

    def run(self, event_stream: Stream):
        """
        Applies the evaluation mechanism to detect the predefined patterns in a given stream of events.
        Returns the total time elapsed during evaluation.
        """
        self.__pattern_matches = Stream()
        start = datetime.now()
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
