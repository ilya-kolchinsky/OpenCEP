from abc import ABC, abstractmethod
from typing import List


class DeviationAwareTester(ABC):
    """
    Abstract class for deviation aware testing function
    """
    def __init__(self, t):
        self._t = t

    @abstractmethod
    def is_deviated_by_t(self, new_statistics, prev_statistics):
        raise NotImplementedError()


class ArrivalRatesDeviationAwareTester(DeviationAwareTester):
    """
    Checks for deviations in the arrival rate by a factor of t.
    """

    def is_deviated_by_t(self, new_statistics: List[int], prev_statistics: List[int]):

        for i in range(len(new_statistics)):
            if prev_statistics[i] * (1 + self._t) < new_statistics[i] or \
                    prev_statistics[i] * (1 - self._t) > new_statistics[i]:
                return True
        return False


class SelectivityDeviationAwareOptimizerTester(DeviationAwareTester):
    """
    Checks for deviations in the selectivity by a factor of t.
    """

    def is_deviated_by_t(self, new_statistics: List[List[float]], prev_statistics: List[List[float]]):

        for i in range(len(new_statistics)):
            for j in range(len(new_statistics[i])):
                if prev_statistics[i][j] * (1 + self._t) < new_statistics[i][j] or \
                        prev_statistics[i][j] * (1 - self._t) > new_statistics[i][j]:
                    return True
        return False

