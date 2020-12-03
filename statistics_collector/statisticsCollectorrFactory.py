
class StatisticsFactory:

    @staticmethod
    def register_statistics_type(statistic_types, event_types):

        stats = []

        for stat_type in statistic_types:
            if stat_type == StatisticsTypes.ARRIVAL_RATES:
                stats.append(ArrivalRates())

            elif stat_type == StatisticsTypes.FREQUENCY_DICT:
                stats.append(Frequency(event_types))

            elif stat_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
                stats.append(SelectivityMatrixAndArrivedRates())

        return stats
