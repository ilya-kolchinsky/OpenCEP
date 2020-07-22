from misc.StatisticsTypes import StatisticsTypes
from optimizer.ReoptimizingDecision import InvariantBasedDecision


class GreedyAlgorithmBasedInvariants(InvariantBasedDecision):
    def __init__(self):
        self.invariants = []  # is a list containing elements: (event_a, event_b, compare_sign: CompareSigns)

    @staticmethod
    def deciding_condition(building_block, SC_data, approved_events):
        event1 = building_block[0]
        event2 = building_block[1]
        event1_value = SC_data.rates[event1] * SC_data.selectivity_matrix[event1][event1]
        event2_value = SC_data.rates[event2] * SC_data.selectivity_matrix[event2][event2]
        for p_k in approved_events:
            event1_value *= SC_data.selectivity_matrix[p_k][event1]
            event2_value *= SC_data.selectivity_matrix[p_k][event2]
        return event1_value <= event2_value

    def decision(self, SC_data):
        """
        Checking if any of the invariants has been violated. If yes then initiate reoptimization
        """
        approved_events = []
        if len(self.invariants) == 0:
            return True
        else:
            for building_block in self.invariants:
                """
                I need to see how i'll receive the information about the events from the statistics collector
                and then take the rates of the events in the deciding block and call self.__compare_events
                with their rates
                """
                if not self.deciding_condition(building_block, SC_data, approved_events):
                    approved_events.clear()
                    return True
                approved_events.append(building_block[0])
        approved_events.clear()
        return False

    def gen_new_invariants(self, ordered_events):
        """
        Inserting the ordered events into as invariants
        """
        self.invariants = []
        for i in range(len(ordered_events) - 1):  # MAKE SURE RUNS ACCORDINGLY (need to run until the before last element)
            self.invariants.append((ordered_events[i], ordered_events[i + 1]))
        return


def create_invariant_based_on_data_type(SC_data_type: StatisticsTypes):
    if SC_data_type == StatisticsTypes.ARRIVAL_RATES:
        return GreedyAlgorithmBasedInvariants()
    else:
        raise NotImplementedError()

