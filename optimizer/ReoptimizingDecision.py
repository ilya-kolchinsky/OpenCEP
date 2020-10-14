from enum import Enum
import time
from abc import ABC
from statisticsCollector.Stat import Stat


class ReoptimizationDecisionTypes(Enum):
    """
    The various methods for deciding whether reoptimization is needed.
    """
    UNCONDITIONAL_PERIODICAL_ADAPTATION = 0,
    RELATIVE_THRESHOLD_BASED = 1,
    INVARIANT_BASED = 2


class UnconditionalPeriodicalAdaptationDecision:
    """
    Changing the current plan every time_limit seconds
    """
    def __init__(self, time_limit):
        self.time_limit = time_limit
        self.start_time = None

    def decision(self, stat: Stat) -> bool:
        """
        Doesn't need any parameters, only here just for generics
        """
        if self.start_time is None or time.time() - self.start_time > self.time_limit:
            self.start_time = time.time()
            return True
        else:
            return False


class RelativeThresholdBasedDecision:
    def __init__(self, threshold):
        self.threshold = threshold
        self.generic_data = None

    def decision(self, stat: Stat) -> bool:
        """
        Go over the previous data and the current data and checks if the relative difference between them is
        higher than the threshold. If yes, returns True, otherwise returns No
        """
        previous_data = self.generic_data
        self.generic_data = stat.get_generic_data()
        if previous_data is None:
            return True     # Meaning CEP doesn't have a plan yet and it needs to be generated
        else:
            for cur_data, prev_data in zip(self.generic_data, previous_data):
                if (prev_data * (self.threshold / 100)) < abs(cur_data - prev_data):
                    return True
        return False


class InvariantBasedDecision(ABC):
    """
    A generic class for creating an invariant based decision.
    """

    def decision(self, stat: Stat) -> bool:
        """
        Checking if any of the invariants has been violated and initiate reoptimization if needed
        """
        pass

    def gen_new_invariants(self, new_plan):
        """
        Getting the new generated plan and create invariants according to it
        """
        pass
