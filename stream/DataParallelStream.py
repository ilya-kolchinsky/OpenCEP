from stream.Stream import OutputStream, InputStream
from queue import Queue

class DataParallelInputStream(InputStream):
    """
    Reads the objects from a queue for data Parallelism
    """

    def __init__(self):
        super().__init__()

    def add_item(self, item: object):
        self._stream.put(item)


class DataParallelOutputStream(OutputStream):
    """
    Writes the objects into a queue for data Parallelism
    """
    def __init__(self):
        super().__init__()
        self._items = set()
        self._mutex = Queue()

    def add_item(self, item: object):
        ######### #todo: need to lock this part
        self._mutex.join()
        self._mutex.put(1)
        item_str = str(item)
        if item_str not in self._items:
            self._items.add(item_str)
            self._stream.put(item)
        self._mutex.task_done()
        ############

        #self._stream.task_done()


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
