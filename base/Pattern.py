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

        if consumption_policies:
            #Check that skip policies are enforced only on SeqOperator
            if consumption_policies.skip:
                if(pattern_structure.get_top_operator() != SeqOperator):
                    raise Exception("Skip policies only work with SeqOperator")
            #Adjust formula if qitems are "strict"
            if consumption_policies.strict:
                self.set_strict_formulas(pattern_structure)

    def set_statistics(self, statistics_type: StatisticsTypes, statistics: object):
        self.statistics_type = statistics_type
        self.statistics = statistics

    def set_strict_formulas(self, pattern_structure: PatternStructure):
        #Adjust formula if qitems are "strict"
        args = pattern_structure.args
        for i in range(len(args)):
            if(args[i].get_top_operator() == SeqOperator):
                self.set_strict_formulas(args[i])
        if(pattern_structure.get_top_operator() == SeqOperator):
            for event_name in self.consumption_policies.strict:
                for i in range(len(args) - 1):
                    if(args[i + 1].name == event_name and args[i].get_top_operator() == QItem and args[i + 1].get_top_operator() == QItem):
                        self.condition = AndFormula(
                            self.condition,
                            EqFormula(
                                IdentifierTerm(args[i].name, lambda x: x["Index"]), 
                                MinusTerm(IdentifierTerm(args[i + 1].name, lambda x: x["Index"]), AtomicTerm(1))
                            )
                        )