from base.Event import Event
from misc.StatisticsTypes import StatisticsTypes
from optimizer import Optimizer
from statistics_collector.ArrivalRates import ArrivalRates
from statistics_collector.Frequency import Frequency
from statistics_collector.SelectivityMatrixAndArrivalRates import SelectivityMatrixAndArrivedRates
from base import Pattern


class StatisticsCollector:

    def __init__(self, optimizer: Optimizer, statistics: List[Statistics], pattern: Pattern):
        self.__optimizer = optimizer
        self.__statistics = statistics

        # self.__statistic_types = statistic_types
        # self.currStatistics = StatisticsFactory(statistic_types)  # functions or objects????
        event_types = {primitive_event.eventType for primitive_event in pattern.positive_structure.args}
        # self.__statistics = StatisticsFactory.register_statistics_type(statistic_type, event_types)  # refractor this name!

    def get_events(self, event: Event):
        for stat in self.statistics:
            stat.update(event)

        # in the end of updating:
        self.send_statistics_to_optimizer()

    def send_statistics_to_optimizer(self):
        self.optimizer.dosomething()