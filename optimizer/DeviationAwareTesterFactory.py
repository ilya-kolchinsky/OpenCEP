from typing import List
from misc.StatisticsTypes import StatisticsTypes
from optimizer import DeviationAwareTester


class DeviationAwareTesterFactory:
    """
    Creates a changes aware tester according to the specification.
    """

    @staticmethod
    def create_deviation_aware_tester(statistics_type: StatisticsTypes or List[StatisticsTypes], t: float):
        if statistics_type == StatisticsTypes.ARRIVAL_RATES:
            return DeviationAwareTester.ArrivalRatesDeviationAwareTester(t)
        if statistics_type == StatisticsTypes.SELECTIVITY_MATRIX:
            return DeviationAwareTester.SelectivityDeviationAwareOptimizerTester(t)
