from enum import Enum


class StatisticsTypes(Enum):
    """
    Type of event statistics provided inside a pattern.
    """
    ARRIVAL_RATES = 1
    SELECTIVITY_MATRIX = 2
