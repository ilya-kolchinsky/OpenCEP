from base.DataFormatter import DataFormatter
from base.Event import Event
from queue import Queue


class Stream:
    """
    Represents a generic stream of objects.
    """
    def __init__(self):
        self.__stream = Queue()

    def __next__(self):
        next_item = self.__stream.get(block=True)  # Blocking get
        if next_item is None:
            raise StopIteration()
        return next_item

    def __iter__(self):
        return self

    def add_item(self, item: object):
        self.__stream.put(item)

    def close(self):
        self.__stream.put(None)

    def duplicate(self):
        ret = Stream()
        ret.__stream.queue = self.__stream.queue.copy()
        return ret

    def get_item(self):
        return self.__next__()

    def count(self):
        return self.__stream.qsize()

    def first(self):
        return self.__stream.queue[0]

    def last(self):
        x = self.__stream.queue[-1]
        if x is None:  # if stream is closed last is None. We need the one before None.
            x = self.__stream.queue[-2]
        return x


def file_input(file_path: str, data_formatter: DataFormatter) -> Stream:
    """
    Receives a file and returns a stream of events.
    "filepath": the path to the file that is to be read.
    The file will be parsed as so:
    * Each line will be a different event
    * Each line will be split on "," and the resulting array will be stored in an "Event",
      and the keys are determined from the given list "KeyMap".
    """
    with open(file_path, "r") as f:
        content = f.readlines()
    events = Stream()
    for line in content:
        events.add_item(Event(line, data_formatter))
    events.close()
    return events


def file_output(matches: list, output_file_name: str = 'matches.txt'):
    """
    Writes output matches to a file in the subfolder "Matches".
    It supports any iterable as output matches.
    """
    with open("test/Matches/" + output_file_name, 'w') as f:
        for match in matches:
            for event in match.events:
                f.write("%s\n" % event.payload)
            f.write("\n")