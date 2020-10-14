from statisticsCollector.StatisticsTypes import StatisticsTypes
from optimizer.ReoptimizingDecision import InvariantBasedDecision
from statisticsCollector.Stat import Stat


class GreedyAlgorithmBasedInvariants(InvariantBasedDecision):
    def __init__(self):
        self.invariants = []  # is a list containing elements: (event_a, event_b, list of already chosen events)

    @staticmethod
    def deciding_condition(building_block, stat: Stat):
        """
        Checks if an invariant has been violated according to the Greedy algorithm for order-based plans
        """
        if building_block[1] is None:
            return True
        event1 = building_block[0]
        event2 = building_block[1]
        already_chosen_events = building_block[2]
        event1_value = stat.arrival_rates[event1] * stat.selectivity_matrix[event1][event1]
        event2_value = stat.arrival_rates[event2] * stat.selectivity_matrix[event2][event2]
        for p_k in already_chosen_events:
            event1_value *= stat.selectivity_matrix[p_k][event1]
            event2_value *= stat.selectivity_matrix[p_k][event2]
        return event1_value <= event2_value

    def decision(self, stat: Stat):
        """
        Checking if any of the invariants has been violated. If yes then returns True, else, returns False
        """
        if len(self.invariants) == 0:  # Meaning it's the first reoptimization decision and there are not invariants yet
            return True
        else:
            for building_block in self.invariants:
                if not self.deciding_condition(building_block, stat):
                    return True
        return False

    def set_new_invariants(self, new_invariants):
        """
        Inserting the ordered events into as invariants
        """
        self.invariants = new_invariants


def create_invariant_based_on_data_type(statistics_type: StatisticsTypes):
    if statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
        return GreedyAlgorithmBasedInvariants()
    else:
        raise NotImplementedError()
