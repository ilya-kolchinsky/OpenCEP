from abc import ABC

from base.DataFormatter import DataFormatter
from stream.Stream import InputStream, OutputStream


class EvaluationMechanism(ABC):
    """
    Every evaluation mechanism must inherit from this class and implement the abstract methods.
    """
    def eval(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """
        Receives an input stream of events and outputs the detected pattern matches into a given output stream.
        """
        raise NotImplementedError()

    def get_structure_summary(self):
        """
        Returns an object summarizing the structure of this evaluation mechanism.
        """
        raise NotImplementedError()
