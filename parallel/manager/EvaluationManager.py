"""
This file contains the interface that must be implemented by an OpenCEP evaluation manager.
An evaluation manager is a component responsible for parallel and/or distributed execution of the CEP functionality.
It internally activates and uses a CEP evaluation mechanism.
"""
from abc import ABC

from stream.Stream import InputStream, OutputStream
from base.DataFormatter import DataFormatter


class EvaluationManager(ABC):
    """
    The root class of the evaluation manager hierarchy.
    """
    def eval(self, event_stream: InputStream, pattern_matches: OutputStream, data_formatter: DataFormatter):
        """
        Utilizes a (possibly parallelized) evaluation mechanism to extract pattern matches from the given input stream
        into a given output stream.
        """
        raise NotImplementedError()

    def get_pattern_match_stream(self):
        """
        Returns the most recently used pattern match stream.
        """
        raise NotImplementedError()

    def get_structure_summary(self):
        """
        Returns a string containing a short description of the underlying evaluation mechanism structure
        """
        raise NotImplementedError()
