from enum import Enum
import time
from misc.StatisticsTypes import StatisticsTypes
from abc import ABC
from statisticsCollector.StatisticsCollector import Stat


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


class StaticThresholdBasedDecision(ABC):
    def __init__(self, threshold):
        self.threshold = threshold

    def decision(self, stat: Stat) -> bool:
        """
        Go over the previous data and the current data and checks if the difference between them is
        higher than the threshold. If yes, returns True, otherwise returns No
        """
        pass


class InvariantBasedDecision(ABC):
    """
    A generic class for creating an invariant based decision.
    """

    def decision(self, stat: Stat) -> bool:
        """
        Checking if any of the invariants has been violated. If yes then initiate reoptimization
        """
        pass

    def gen_new_invariants(self, new_plan):
        """
        Getting the new generated plan and create invariants according to it
        """
        pass

