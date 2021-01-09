from base.Event import Event
from misc.StatisticsTypes import StatisticsTypes
from optimizer import Optimizer
from statistics_collector.ArrivalRates import ArrivalRates
from statistics_collector.SelectivityMatrixAndArrivalRates import SelectivityMatrixAndArrivedRates
from base import Pattern


class StatisticsCollector:

    def __init__(self, pattern: Pattern):
        self.__statistics = pattern.statistics

    def event_handler(self, event: Event):
        for stat in self.__statistics:
            stat.update(event)