from abc import ABC
from misc.IOUtils import Stream
from statisticsCollector.StatisticsCollector import StatisticsCollector


class EvaluationMechanism(ABC):
    """
    Every evaluation mechanism must inherit from this class and implement the 'eval' function, receiving an input
    stream of events and putting the detected pattern matches into a given output stream.
    """
    def eval(self, events: Stream, new_order_for_tree, matches: Stream, statistics_collector: StatisticsCollector):
        pass
