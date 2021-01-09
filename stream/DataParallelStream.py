from stream.Stream import *
from queue import Queue

class DataParallelInputStream(InputStream):
    """
    Reads the objects from a queue for data Parallelism
    """

    def __init__(self):
        super().__init__()

    def add_item(self, item: object):
        self._stream.put(item)


class DataParallelOutputBuffer(Stream):
    """
    Writes the objects into a queue for data Parallelism
    """
    def __init__(self, matches: InputStream):
        super().__init__()
        self._duplicated = set()
        self._mutex = Queue()
        self._matches = matches

    def add_item(self, item: list):
        ######### #todo: need to lock this part
        super().add_item(item)


    def close(self):
        super().close()

    def last(self):
        x = self._stream.queue[-1]
        return x


class TreeParallelOutputStream(OutputStream):
    """
    Writes the objects into a queue for data Parallelism
    """

    def __init__(self):
        super().__init__()

    def add_item(self, item: object):
        self._stream.put(item)

    def close(self):
        super().close()

    def last(self):
        x = self._stream.queue[-1]
        return x
