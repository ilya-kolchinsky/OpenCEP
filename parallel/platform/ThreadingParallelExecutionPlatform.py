"""
Provides parallelization functionality based on Python threading library.
"""
import threading

from parallel.platform.ParallelExecutionPlatform import ParallelExecutionPlatform, ParallelExecutionUnit


class ThreadingParallelExecutionPlatform(ParallelExecutionPlatform):
    """
    Creates execution unit objects based on Python threads.
    """
    @staticmethod
    def create_parallel_execution_unit(unit_id: int, callback_function: callable, *args, **kwargs):
        new_thread = threading.Thread(target=callback_function, args=args, kwargs=kwargs)
        return ThreadingParallelExecutionUnit(unit_id, new_thread)


class ThreadingParallelExecutionUnit(ParallelExecutionUnit):
    """
    A parallel execution unit wrapping a single Python thread.
    """
    def __init__(self, unit_id: int, thread: threading.Thread):
        super().__init__(unit_id)
        self._thread = thread

    def start(self):
        return self._thread.start()

    def stop(self):
        # TODO: not yet implemented
        return

    def wait(self, timeout: float = None):
        return self._thread.join(timeout)

    def send(self, data: object):
        # TODO: not yet implemented
        return

    def receive(self, timeout: float = None):
        # TODO: not yet implemented
        return
