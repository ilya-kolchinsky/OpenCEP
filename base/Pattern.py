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
        #nathan
        #self.structure = pattern_structure

        #self.condition = pattern_matching_condition
        self.window = time_window
        self.statistics_type = StatisticsTypes.NO_STATISTICS
        self.statistics = None

        """
        nathan:
        origin structure is the pattern structure that we get in params - containing all the fields
        structure is the origin structure without the negative events
        negative event contains only the negative events of the pattern
        """
        self.origin_structure = pattern_structure
        self.structure = pattern_structure.create_top_operator()
        self.negative_event = pattern_structure.create_top_operator()

        self.split_structures()

        #self.negative_condition_list = list()
        self.condition = pattern_matching_condition
        #self.split_formula()

    def set_statistics(self, statistics_type: StatisticsTypes, statistics: object):
        self.statistics_type = statistics_type
        self.statistics = statistics

    def split_structures(self):
        for i in range(len(self.origin_structure.get_args())):
            p = self.origin_structure.get_args()[i]
            if type(p) == NegationOperator:
                self.negative_event.add_arg(p)
            else:
                self.structure.add_arg(p)
"""
    def split_formula(self):
        for i in self.negative_event.get_args():
            self.negative_condition_list.append(self.condition.get_formula_of(i.get_event_name()))
"""