from functools import reduce
from typing import List

from base.Event import Event
from base.Formula import Formula, EqFormula, IdentifierTerm, MinusTerm, AtomicTerm, AndFormula
from base.PatternStructure import PatternStructure
from datetime import timedelta
from misc.StatisticsTypes import StatisticsTypes
from base.PatternStructure import SeqOperator, QItem
from misc.ConsumptionPolicies import ConsumptionPolicies

class Pattern:
    """
    A pattern has several fields:
    - a structure represented by a tree of operators over the primitive events (e.g., SEQ(A,B*, AND(C, NOT(D), E)));
    - a condition to be satisfied by the primitive events (might consist of multiple nested conditions);
    - a time window for the pattern matches to occur within.
    - a ConsumptionPolicies object that contains the policies that filter certain partial matches
    A pattern can also carry statistics with it, in order to enable advanced
    tree construction mechanisms - this is hopefully a temporary hack.
    """
    def __init__(self, pattern_structure: PatternStructure, pattern_matching_condition: Formula = None,
                 time_window: timedelta = timedelta.max, consumption_policies: ConsumptionPolicies = None):        
        self.structure = pattern_structure
        self.condition = pattern_matching_condition
        self.window = time_window
        self.statistics_type = StatisticsTypes.NO_STATISTICS
        self.statistics = None
        self.consumption_policies = consumption_policies

        if consumption_policies is not None and consumption_policies.contiguous is not None:
            self.__init_strict_formulas(pattern_structure)

    def set_statistics(self, statistics_type: StatisticsTypes, statistics: object):
        self.statistics_type = statistics_type
        self.statistics = statistics

    def __init_strict_formulas(self, pattern_structure: PatternStructure):
        """
        Augment the pattern with the contiguity constraints specified as a part of the consumption policy.
        """
        if pattern_structure.get_top_operator() == QItem:
            return
        args = pattern_structure.args
        for i in range(len(args)):
            self.__init_strict_formulas(args[i])
        if pattern_structure.get_top_operator() != SeqOperator:
            return
        for contiguous_sequence in self.consumption_policies.contiguous:
            for i in range(len(contiguous_sequence) - 1):
                for j in range(len(args) - 1):
                    if args[i].get_top_operator() != QItem or args[i + 1].get_top_operator() != QItem:
                        continue
                    if contiguous_sequence[i] != args[j].name:
                        continue
                    if contiguous_sequence[i + 1] != args[j + 1].name:
                        raise Exception("Contiguity constraints contradict the pattern structure: " +
                                        "%s must follow %s" % (contiguous_sequence[i], contiguous_sequence[i + 1]))
                    self.__add_contiguity_condition(args[i].name, args[i + 1].name)

    def __add_contiguity_condition(self, first_name: str, second_name: str):
        """
        Augment the pattern condition with a contiguity constraint between the given event names.
        """
        self.condition = AndFormula(
            self.condition,
            EqFormula(
                IdentifierTerm(first_name, lambda x: x[Event.INDEX_ATTRIBUTE_NAME]),
                MinusTerm(IdentifierTerm(second_name, lambda x: x[Event.INDEX_ATTRIBUTE_NAME]), AtomicTerm(1))
            )
        )

    def extract_flat_sequences(self) -> List[List[str]]:
        """
        Returns a list of all flat sequences in the pattern.
        For now, nested operators inside the scope of a sequence are not supported. For example,
        in the pattern SEQ(A,AND(B,C),D) there are two hidden sequences [A,B,D] and [A,C,D], but this method will
        not return them as of now.
        """
        return self.__extract_flat_sequences_aux(self.structure)

    def __extract_flat_sequences_aux(self, pattern_structure: PatternStructure) -> List[List[str]]:
        """
        An auxiliary method for extracting flat sequences from the pattern.
        """
        if pattern_structure.get_top_operator() == QItem:
            return None
        if pattern_structure.get_top_operator() == SeqOperator:
            # note the double brackets - we return a list composed of a single list representing this sequence
            return [[arg.name for arg in pattern_structure.args if arg.get_top_operator() == QItem]]
        # the pattern is a composite pattern but not a flat sequence
        result = []
        for arg in pattern_structure.args:
            nested_sequences = self.__extract_flat_sequences_aux(arg)
            if nested_sequences is not None:
                result.extend(nested_sequences)
        return result
