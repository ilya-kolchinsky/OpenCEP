from typing import List
from statistics_collector.StatisticsTypes import StatisticsTypes
from optimizer import DeviationAwareTester


class DeviationAwareTesterFactory:
    """
    Creates a deviation aware tester according to the specifications.
    """

    @staticmethod
    def create_deviation_aware_tester(statistics_type: StatisticsTypes or List[StatisticsTypes], t: float):
        if statistics_type == StatisticsTypes.ARRIVAL_RATES:
            return DeviationAwareTester.ArrivalRatesDeviationAwareTester(t)
        if statistics_type == StatisticsTypes.SELECTIVITY_MATRIX:
            return DeviationAwareTester.SelectivityDeviationAwareOptimizerTester(t)
