from base.Formula import Formula
from base.PatternStructure import PatternStructure
from datetime import timedelta
from misc.StatisticsTypes import StatisticsTypes

from base.Formula import GreaterThanFormula, SmallerThanFormula, SmallerThanEqFormula, GreaterThanEqFormula, MulTerm, EqFormula, IdentifierTerm, AtomicTerm, AndFormula, TrueFormula
from base.PatternStructure import AndOperator, SeqOperator, QItem, NegationOperator

class Pattern:
    """
    A pattern has several fields:
    - a structure represented by a tree of operators over the primitive events (e.g., SEQ(A,B*, AND(C, NOT(D), E)));
    - a condition to be satisfied by the primitive events (might consist of multiple nested conditions);
    - a time window for the pattern matches to occur within.
    A pattern can also carry statistics with it, in order to enable advanced
    tree construction mechanisms - this is hopefully a temporary hack.
    """
    def __init__(self, pattern_structure: PatternStructure, pattern_matching_condition: Formula = None,
                 time_window: timedelta = timedelta.max):
        self.structure = pattern_structure
        self.condition = pattern_matching_condition
        self.window = time_window
        self.statistics_type = StatisticsTypes.NO_STATISTICS
        self.statistics = None
#EVA 17.05
        self.positive_event = pattern_structure.duplicate()
        self.negative_event = pattern_structure.duplicate()
        i = 0
        while i < len(pattern_structure.get_args()):
            p = pattern_structure.get_args()[i]
            if type(p) == NegationOperator:
                self.positive_event.remove_arg(p)
            else:
                self.negative_event.remove_arg(p)
            i += 1

    def set_statistics(self, statistics_type: StatisticsTypes, statistics: object):
        self.statistics_type = statistics_type
        self.statistics = statistics

    #def split_Negation_Event(self):



        #test = patterns[0].structure.get_args()[0].name
