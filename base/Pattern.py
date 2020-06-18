from base.Formula import Formula, EqFormula, IdentifierTerm, MinusTerm, AtomicTerm, AndFormula
from base.PatternStructure import PatternStructure
from datetime import timedelta
from misc.StatisticsTypes import StatisticsTypes
from base.PatternStructure import SeqOperator, StrictSeqOperator

class Pattern:
    """
    A pattern has several fields:
    - a structure represented by a tree of operators over the primitive events (e.g., SEQ(A,B*, AND(C, NOT(D), E)));
    - a condition to be satisfied by the primitive events (might consist of multiple nested conditions);
    - a time window for the pattern matches to occur within.
    - a set of events that limit the event to only appear in a single full match
    A pattern can also carry statistics with it, in order to enable advanced
    tree construction mechanisms - this is hopefully a temporary hack.
    """
    def __init__(self, pattern_structure: PatternStructure, pattern_matching_condition: Formula = None,
                 time_window: timedelta = timedelta.max, single_types: set = None):
        #Adjust formula if qitems are "strict"
        args = pattern_structure.args
        if(pattern_structure.get_top_operator() == StrictSeqOperator):
            for i in range(len(args) - 1):
                args[i + 1].strict = True
            pattern_structure = SeqOperator(args)
        if(pattern_structure.get_top_operator() == SeqOperator):
            for i in range(len(args) - 1):
                if(args[i + 1].strict == True):
                    pattern_matching_condition = AndFormula(
                        pattern_matching_condition,
                        EqFormula(
                            IdentifierTerm(args[i].name, lambda x: x["Index"]), 
                            MinusTerm(IdentifierTerm(args[i + 1].name, lambda x: x["Index"]), AtomicTerm(1))
                        )
                    )
        
        self.structure = pattern_structure
        self.condition = pattern_matching_condition
        self.window = time_window
        self.statistics_type = StatisticsTypes.NO_STATISTICS
        self.statistics = None
        self.single_types = single_types

    def set_statistics(self, statistics_type: StatisticsTypes, statistics: object):
        self.statistics_type = statistics_type
        self.statistics = statistics
