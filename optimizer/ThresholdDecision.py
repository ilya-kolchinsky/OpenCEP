from statisticsCollector.StatisticsTypes import StatisticsTypes
from optimizer.ReoptimizingDecision import StaticThresholdBasedDecision
from statisticsCollector.StatisticsCollector import Stat


class RateBasedStaticThresholdBasedDecision(StaticThresholdBasedDecision):
    """
    Changing the current plan in case the events' arrival_rates are deviating by more then the threshold
    """
    def __init__(self, threshold):
        super().__init__(threshold)
        self.arrival_rates = None

    def decision(self, stat: Stat) -> bool:
        prev_arrival_rates = self.arrival_rates      # temp variable
        self.arrival_rates = stat.arrival_rates
        if prev_arrival_rates is None:
            return True              # Meaning CEP doesn't have a plan yet and it needs to be generated
        else:
            for cur_rate, prev_rate in zip(self.arrival_rates, prev_arrival_rates):
                if abs(cur_rate - prev_rate) > self.threshold:
                    return True
        return False


def create_threshold_based_on_data_type(statistics_type: StatisticsTypes, threshold):
    if statistics_type == StatisticsTypes.ARRIVAL_RATES or statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
        return RateBasedStaticThresholdBasedDecision(threshold)
    else:
        raise NotImplementedError()
