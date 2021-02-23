from typing import List
from misc.StatisticsTypes import StatisticsTypes
from optimizer import ChangesAwareTester


class ChangesAwareTesterFactory:
    """
    Creates a changes aware tester according to the specification.
    """

    @staticmethod
    def create_changes_aware_tester(statistics_type: StatisticsTypes or List[StatisticsTypes], t: float):
        if statistics_type == StatisticsTypes.ARRIVAL_RATES:
            return ChangesAwareTester.ArrivalRatesChangesAwareTester(t)
        if statistics_type == StatisticsTypes.SELECTIVITY_MATRIX:
            return ChangesAwareTester.SelectivityChangesAwareOptimizerTester(t)
