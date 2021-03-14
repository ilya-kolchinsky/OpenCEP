from abc import ABC, abstractmethod
from typing import List


class DeviationAwareTester(ABC):
    """
    An abstract class for deviation-aware testing functions.
    """
    def __init__(self, deviation_threshold: float):
        self._deviation_threshold = deviation_threshold

    @abstractmethod
    def is_deviated_by_t(self, new_statistics, prev_statistics):
        """
        Checks if there was a deviation in one of the statistics by the given factor.
        """
        raise NotImplementedError()


class ArrivalRatesDeviationAwareTester(DeviationAwareTester):
    """
    Checks for deviations in the arrival rates statistics by the given factor.
    """
    def is_deviated_by_t(self, new_statistics: List[int], prev_statistics: List[int]):
        for i in range(len(new_statistics)):
            if prev_statistics[i] * (1 + self._deviation_threshold) < new_statistics[i] or \
                    prev_statistics[i] * (1 - self._deviation_threshold) > new_statistics[i]:
                return True
        return False


class SelectivityDeviationAwareOptimizerTester(DeviationAwareTester):
    """
    Checks for deviations in the selectivity matrix statistics by the given factor.
    """
    def is_deviated_by_t(self, new_statistics: List[List[float]], prev_statistics: List[List[float]]):
        for i in range(len(new_statistics)):
            for j in range(i+1):
                if prev_statistics[i][j] * (1 + self._deviation_threshold) < new_statistics[i][j] or \
                        prev_statistics[i][j] * (1 - self._deviation_threshold) > new_statistics[i][j]:
                    return True
        return False

