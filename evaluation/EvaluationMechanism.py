from abc import ABC
from misc.IOUtils import Stream


class EvaluationMechanism(ABC):
    """
    Every evaluation mechanism must inherit from this class and implement the 'eval' function, receiving an input
    stream of events and putting the detected pattern matches into a given output stream.
    """
    def eval(self, events: Stream, matches: Stream):
        pass
