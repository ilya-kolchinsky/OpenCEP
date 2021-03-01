from stream.Stream import *
from queue import Queue

class DataParallelStream(Stream):
    """
    Reads the objects from a queue for data Parallelism
    """
    def __init__(self):
        super().__init__()

    def close(self):
        pass

    def close_all(self):
        super().close()
