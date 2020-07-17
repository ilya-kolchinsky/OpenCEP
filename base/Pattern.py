from base.Formula import Formula
from base.PatternStructure import PatternStructure
from datetime import timedelta
from misc.StatisticsTypes import StatisticsTypes

from base.Formula import GreaterThanFormula, SmallerThanFormula, SmallerThanEqFormula, GreaterThanEqFormula, MulTerm, EqFormula, IdentifierTerm, AtomicTerm, AndFormula, TrueFormula
from base.PatternStructure import AndOperator, SeqOperator, QItem, NegationOperator, OrOperator

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

        self.window = time_window
        self.statistics_type = StatisticsTypes.NO_STATISTICS
        self.statistics = None
        self.condition = pattern_matching_condition

        """
        origin_structure is the pattern structure that we get in params - containing all the fields.
        structure is the origin structure without the negative events.
        negative_event contains only the negative events of the pattern.
        """
        self.origin_structure = pattern_structure
        self.structure = pattern_structure.create_top_operator()
        self.negative_event = pattern_structure.create_top_operator()

        self.split_structures()


    def set_statistics(self, statistics_type: StatisticsTypes, statistics: object):
        self.statistics_type = statistics_type
        self.statistics = statistics

    def split_structures(self):
        origin_structure_args = self.origin_structure.get_args()
        index = 0
        for i in range(len(origin_structure_args)):
            p = origin_structure_args[i]
            index = p.set_qitem_index(index)
            # the returned index is the one we want to use at next iteration : it's incremented in set_qitem_index
            # no need to do index++
            if type(p) == NegationOperator:
                self.negative_event.add_arg(p)
            else:
                self.structure.add_arg(p)
