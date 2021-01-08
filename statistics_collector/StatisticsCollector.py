from base.Event import Event
from misc.StatisticsTypes import StatisticsTypes
from optimizer import Optimizer
from statistics_collector.ArrivalRates import ArrivalRates
from statistics_collector.SelectivityMatrixAndArrivalRates import SelectivityMatrixAndArrivedRates
from base import Pattern


class StatisticsCollector:

    def __init__(self, statistics, pattern: Pattern):
        # todo all the infrastructure working with pattern  and not with statistics
        self.__statistics = statistics
        self.pattern = pattern

    def event_handler(self, event: Event):
        for stat in self.__statistics:
            stat.update(event)