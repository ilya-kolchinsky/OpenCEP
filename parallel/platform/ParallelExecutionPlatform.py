"""
A generic interface for encapsulating the platform that provides parallelization functionality
(e.g., threads, processes, VMs, physical machines in a cluster, cloud, edge, etc.).
"""

from abc import ABC


class ParallelExecutionPlatform(ABC):
    """
    A wrapper for accessing parallelization capabilities of any platform with such functionality.
    """
    @staticmethod
    def create_parallel_execution_unit(unit_id: int, callback_function: callable, *args, **kwargs):
        """
        Initializes and returns an object representing a single parallel execution unit.
        """
        raise NotImplementedError()


class ParallelExecutionUnit(ABC):
    """
    Represents a single unit of parallel execution (such as a thread, a process, a VM, or a physical server).
    """
    def __init__(self, unit_id: int):
        self._id = unit_id

    def get_id(self):
        """
        Returns an ID of this execution unit.
        """
        return self._id

    def start(self):
        """
        Activates the execution unit.
        """
        raise NotImplementedError()

    def stop(self):
        """
        Stops the execution unit.
        """
        raise NotImplementedError()

    def wait(self, timeout: float = None):
        """
        Waits until the execution unit is complete or the predefined timeout is reached.
        """
        raise NotImplementedError()

    def send(self, data: object):
        """
        Sends a given object to the execution unit.
        """
        raise NotImplementedError()

    def receive(self, timeout: float = None):
        """
        Attempts to receive an object from the execution unit.
        """
        raise NotImplementedError()
