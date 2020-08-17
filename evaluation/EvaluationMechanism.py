from abc import ABC
from misc.IOUtils import Stream


class EvaluationMechanism(ABC):
    """
    Every evaluation mechanism must inherit from this class and implement the abstract methods.
    """
    def eval(self, events: Stream, matches: Stream):
        """
        Receives an input stream of events and outputs the detected pattern matches into a given output stream.
        """

    def get_structure_summary(self):
        """
        Returns an object summarizing the structure of this evaluation mechanism.
        """
        raise NotImplementedError()
