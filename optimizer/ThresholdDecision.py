from misc.StatisticsTypes import StatisticsTypes
from optimizer.ReoptimizingDecision import StaticThresholdBasedDecision


class RateBasedStaticThresholdBasedDecision(StaticThresholdBasedDecision):
    def __init__(self, threshold):
        super().__init__(threshold)
        self.rates = None

    def decision(self, SC_data) -> bool:
        prev_rates = self.rates      # temp variable
        self.rates = SC_data.rates
        if prev_rates is None:
            return True                     # Meaning CEP doesn't have a plan yet and it needs to be generated
        else:
            for cur_rate, prev_rate in zip(self.rates.values(), prev_rates.values()):
                if abs(cur_rate - prev_rate) > self.threshold:
                    return True
        return False


def create_threshold_based_on_data_type(SC_data_type: StatisticsTypes, threshold):
    if SC_data_type == StatisticsTypes.ARRIVAL_RATES:
        return RateBasedStaticThresholdBasedDecision(threshold)
    else:
        raise NotImplementedError()
