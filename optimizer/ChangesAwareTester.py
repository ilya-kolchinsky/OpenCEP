from abc import ABC, abstractmethod


class ChangesAwareTester(ABC):
    """
    Abstract class for changes aware testing function
    """
    def __init__(self, t):
        self._t = t

    @abstractmethod
    def is_changed_by_t(self, new_statistics, prev_statistics):
        raise NotImplementedError()


class ArrivalRatesChangesAwareTester(ChangesAwareTester):
    """
    Checks for changes in the arrival rate by a factor of t.
    """

    def is_changed_by_t(self, new_statistics, prev_statistics):

        for i in range(len(new_statistics)):
            if prev_statistics[i] * (1 + self._t) < new_statistics[i] or \
                    prev_statistics[i] * (1 - self._t) > new_statistics[i]:
                return True
        return False


class SelectivityChangesAwareOptimizerTester(ChangesAwareTester):
    """
    Checks for changes in the selectivity by a factor of t.
    """

    def is_changed_by_t(self, new_statistics, prev_statistics):

        for i in range(len(new_statistics)):
            for j in range(len(new_statistics[i])):
                if prev_statistics[i][j] * (1 + self._t) < new_statistics[i][j] or \
                        prev_statistics[i][j] * (1 - self._t) > new_statistics[i][j]:
                    return True
        return False

