from base.Formula import Formula
from base.PatternStructure import PatternStructure
from datetime import timedelta
from misc.StatisticsTypes import StatisticsTypes


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

    def set_statistics(self, statistics_type: StatisticsTypes, statistics: object):
        self.statistics_type = statistics_type
        self.statistics = statistics

    def __repr__(self):
        return "Pattern is {} with condition {} and time window is {}".format(
            self.structure, self.condition, self.window
        )
