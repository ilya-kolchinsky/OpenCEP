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

        if consumption_policies is not None:
            # Check that skip policies are enforced only on SeqOperator
            if consumption_policies.skip is not None and pattern_structure.get_top_operator() != SeqOperator:
                raise Exception("Skip policies only work with SeqOperator")
            if consumption_policies.contiguous is not None:
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
