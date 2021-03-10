from enum import Enum


class StatisticsTypes(Enum):
    """
    Type of event statistics provided inside a pattern (NO_STATISTICS if nothing is available).
    """
    NO_STATISTICS = 0
    SELECTIVITY_MATRIX_AND_ARRIVAL_RATES = 1
    ARRIVAL_RATES = 2
