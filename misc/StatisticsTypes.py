from enum import Enum


class StatisticsTypes(Enum):
    """
    Type of event statistics provided inside a pattern (NO_STATISTICS if nothing is available).
    """
    NO_STATISTICS = 0
    FREQUENCY_DICT = 1
    SELECTIVITY_MATRIX_AND_ARRIVAL_RATES = 2
    ARRIVAL_RATES = 3
