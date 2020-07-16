from enum import Enum
import time
from optimizer.Invariants import create_invariant_based_on_data_type
from optimizer.Optimizer import StatisticsCollectorDataTypes

class ReoptimizationDecisionTypes(Enum):
    """
    The various methods for deciding whether reoptimization is needed.
    """
    UNCONDITIONAL_PERIODICAL_ADAPTATION = 0,
    STATIC_THRESHOLD_BASED = 1,
    INVARIANT_BASED = 2


class UnconditionalPeriodicalAdaptationDecision:
    def __init__(self, time_limit):
        self.time_limit = time_limit
        self.start_time = time.time()

    def decision(self, SC_data: StatisticsCollectorData, data_type) -> bool:
        """
        Doesn't need any parameters, they are here just for generics
        """
        if time.time() - self.start_time > self.time_limit:
            self.start_time = time.time()
            return True
        else:
            return False

class StaticThresholdBasedDecision:
    def __init__(self, threshold):
        self.threshold = threshold
        self.prev_data = None
        #self.events_rate =     Should be an array of rates in the order (by name) of the events and  initialized to something

    def decision(self, SC_data: StatisticsCollectorData, data_type) -> bool:
        """
        Go over self.events_rate and current_events_rate and check if the difference between them is
        higher than the threshold. If yes then initiate reoptimization
        """
        previous_data = self.prev_data      # temp variable
        self.prev_data = SC_data
        if previous_data is None:
            return True                     # Meaning CEP doesn't have a plan yet and it needs to be generated
        else:
            for event_data in SC_data.datas:
                if abs(event_data - previous_data) > self.threshold:
                    return True
        return False


class InvariantBasedDecision:
    def __init__(self, SC_data_type: StatisticsCollectorDataTypes):
        """
        Initializing the invariants based on the paper- with BBCs and DSCs
        """
        self.invariant_based_on_type = create_invariant_based_on_data_type(SC_data_type)
        #self.data_type = data_type
        #self.prev_data = None

    def decision(self, SC_data: StatisticsCollectorData, data_type) -> bool:
        """
        Checking if any of the invariants has been violated. If yes then initiate reoptimization
        """
        if self.prev_data is None:
            return True
        else:
            return self.invariant_based_on_type.decision(SC_data)

    def gen_new_invariants(self, new_plan):
        """
        Getting the new plan generated and saving it as prev_plan for later checking if an invariant has been
        violated
        """
        self.invariant_based_on_type.gen_new_invariants(new_plan)
"""
It's IMPORTANT to separate the decision function for each type of evaluation mechanism type.
For a Tree based plans, ordered based plan...
"""
