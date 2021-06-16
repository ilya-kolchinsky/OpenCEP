from queue import Queue


class Stream:
    """
    Represents a generic stream of objects.
    """
    def __init__(self):
        self._stream = Queue()
        self._reference_count = 1

    def __next__(self):
        next_item = self._stream.get(block=True)  # Blocking get
        if next_item is None:
            raise StopIteration()
        return next_item

    def __iter__(self):
        return self

    def add_item(self, item: object):
        self._stream.put(item)

    def close(self):
        self._reference_count -= 1
        if not self._reference_count:
            self._stream.put(None)
        return self._reference_count == 0  #is_close

    def duplicate(self):
        ret = Stream()
        ret._stream.queue = self._stream.queue.copy()
        return ret

    def get_item(self):
        return self.__next__()

    def count(self):
        return self._stream.qsize()

    def first(self):
        return self._stream.queue[0]

    def last(self):
        x = self._stream.queue[-1]
        if x is None:  # if stream is closed last is None. We need the one before None.
            x = self._stream.queue[-2]
        return x

    def get_reference(self):
        self._reference_count += 1
        return self

    # def join(self):
    #     self._stream.join()
    #
    # def task_done(self):
    #     self._stream.task_done()





class InputStream(Stream):
    """
    A stream receiving its items from some external source.
    """
    def add_item(self, item: object):
        raise Exception("Unsupported operation")


class OutputStream(Stream):
    """
    A stream sending its items to some external sink.
    """
    def get_item(self):
        raise Exception("Unsupported operation")

    def first(self):
        raise Exception("Unsupported operation")

    def last(self):
        raise Exception("Unsupported operation")
