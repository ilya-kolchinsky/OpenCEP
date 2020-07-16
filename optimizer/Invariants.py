from enum import Enum
from optimizer.Optimizer import StatisticsCollectorDataTypes

class CompareSigns(Enum):
    SMALLER = 0 ,
    SMALLER_EQUAL = 1

class RateBasedInvariants:
    def __init__(self):
        self.invariants = [] # is a list containing elements: (event_a, event_b, compare_sign: CompareSigns)

    @staticmethod
    def __compare_events(event_a_rate, event_b_rate, compare_sign: CompareSigns):
        if compare_sign == 0:
            return event_a_rate < event_b_rate
        if compare_sign == 1:
            return event_a_rate <= event_b_rate
        else:
            raise NotImplementedError()

    def decision(self, SC_data: StatisticsCollectorData):
        if len(self.invariants) == 0:
            self.gen_new_invariants(SC_data)
            return True
        else:
            for deciding_condition in self.invariants:
                """
                I need to see how i'll receive the information about the events from the statistics collector
                and then take the rates of the events in the deciding block and call self.__compare_events
                with their rates
                """
                if not self.__compare_events(EVENT_A_RATE, EVENT_B_RATE, deciding_condition[2]):
                    return True
        return False

    def gen_new_invariants(self, events_sorted):
        self.invariants = []
        for i in range(len(events_sorted) - 1): # MAKE SURE RUNS ACCORDINGLY (need to run until the before last element)
            if events_sorted[i] < events_sorted[i + 1]:
                self.invariants.append((events_sorted[i], events_sorted[i + 1],CompareSigns.SMALLER))
            elif events_sorted[i] <= events_sorted[i + 1]:
                self.invariants.append((events_sorted[i], events_sorted[i + 1],CompareSigns.SMALLER_EQUAL))
            else:       # JUST FOR TESTING #@!#@!#@!#!@#!@#
                print("Error : events_sorted not sorted!!")
        return

def create_invariant_based_on_data_type(SC_data_type: StatisticsCollectorDataTypes):
    if SC_data_type == StatisticsCollectorDataTypes.EVENTS_RATE:
        return RateBasedInvariants()
    else:
        raise NotImplementedError()