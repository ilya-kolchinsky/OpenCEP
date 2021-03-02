
"""
 Data parallel algorithms
"""
from abc import ABC
from stream.DataParallelStream import *
import math
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import \
    EvaluationMechanismParameters, EvaluationMechanismFactory
from base.DataFormatter import DataFormatter
from queue import Queue
from base.PatternMatch import *
import time
from threading import Lock


class DataParallelAlgorithm(ABC):
    """
        An abstract base class for all  data parallel evaluation algorithms.
    """

    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters,
                 platform):
        self._platform = platform
        self._data_formatter = None
        self._units_number = units_number
        self._units = []
        self._eval_trees = []
        self._events = None
        self._events_list = []
        self._stream_unit = platform.create_parallel_execution_unit(
            unit_id=self._units_number - 1,
            callback_function=self._stream_divide)
        self._matches = None
        self._patterns = [patterns] if isinstance(patterns, Pattern) else \
            patterns

        for _ in range(0, self._units_number - 1):
            self._eval_trees.append(EvaluationMechanismFactory.build_eval_mechanism(eval_mechanism_params, patterns))
            self._events_list.append(Stream())

    def eval_algorithm(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """
            Activates the algorithm evaluation mechanism
        """
        self._events = events
        self._data_formatter = data_formatter
        self._matches = matches
        self._stream_unit.start()
        for i in range(self._units_number - 1):
            thread = self._platform.create_parallel_execution_unit(
                unit_id=i,
                callback_function=self._eval_thread,
                thread_id=i,
                data_formatter=data_formatter)
            self._units.append(thread)
            thread.start()

    def _eval_tread(self):
        """
            Activates the unit evaluation mechanism
        """
        raise NotImplementedError()

    def _stream_divide(self):
        """
            Divide the input stream into the appropriate units
        """
        raise NotImplementedError()

    def get_structure_summary(self):
        return self._eval_trees[0].get_structure_summary()


class HirzelAlgorithm(DataParallelAlgorithm, ABC):
    """
        A class for data parallel evaluation Hirzel algorithm
    """

    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters,
                 platform, key: str):
        super().__init__(units_number, patterns, eval_mechanism_params,
                         platform)
        self.key = key

    def eval_algorithm(self, events: InputStream, matches: OutputStream,
                       data_formatter: DataFormatter):

        event = Event(events.first(), data_formatter)
        key_val = event.payload[self.key]
        if not isinstance(key_val, (int, float)):
            raise Exception("key %s has no numeric value" % (self.key,))
        super().eval_algorithm(events, matches, data_formatter)

        for t in self._units:
            t.wait()

        self._matches.close()

    """
        Divide the input stream into calculation units according to the values of the key
    """
    def _stream_divide(self):

        for event_raw in self._events:
            event = Event(event_raw, self._data_formatter)
            index = int(event.payload[self.key] % (self._units_number - 1))
            self._events_list[index].add_item(event_raw)

        for stream in self._events_list:
            stream.close()

    def _eval_thread(self, thread_id: int, data_formatter: DataFormatter):
        self._eval_trees[thread_id].eval(self._events_list[thread_id],
                                         self._matches, data_formatter, False)


class RIPAlgorithm(DataParallelAlgorithm, ABC):
    """
        A class for data parallel evaluation RIP algorithm
    """

    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters,
                 platform, multiple):

        super().__init__(units_number, patterns, eval_mechanism_params,
                         platform)
        self._eval_mechanism_params = eval_mechanism_params
        self.__matches_handler = Stream()
        self._init_time = None

        if isinstance(patterns, Pattern):
            patterns = [patterns]
        max_window = patterns[0].window
        for k in range(1, len(patterns)):
            if patterns[k].window > max_window:
                max_window = patterns[k].window
        if multiple < 2:
            raise Exception("Time window is too small")
        self.__time_slot = multiple * max_window
        self.__shared_time = max_window
        self.__algorithm_start_time = -1
        self.__start_list = [Stream() for _ in
                             range(self._units_number - 1)]
        self.__start_queue = Queue()
        self.__streams_queue = Queue()
        self.__thread_pool = Queue()

        for i in range(units_number - 1):
            self.__thread_pool.put(i)

        self._mutex = Queue()
        self.__duplicated_matches = list()

    """
        Divide the input stream into calculation units according to events times
    """
    def _stream_divide(self):

        try:
            event_raw = self._events.get_item()
            cur_event = Event(event_raw, self._data_formatter)
        except StopIteration:
            raise Exception("Stream has no data")

        curr_time = start_time = self.__algorithm_start_time = cur_event.timestamp
        end_time = start_time + self.__time_slot
        stream_s = Stream()
        check_data = True

        while check_data:
            stream = stream_s.duplicate()
            stream_s = Stream()
            while curr_time <= end_time and check_data:
                stream.add_item(event_raw)
                if curr_time >= end_time - self.__shared_time:
                    stream_s.add_item(event_raw)
                try:
                    event_raw = self._events.get_item()
                    cur_event = Event(event_raw, self._data_formatter)
                    curr_time = cur_event.timestamp
                except StopIteration:
                    check_data = False
            stream.close()
            if stream.count() > 0:
                self.__streams_queue.put_nowait(stream.duplicate())
                self.__start_queue.put_nowait(start_time)
            start_time = end_time - self.__shared_time
            end_time = start_time + self.__time_slot

        while not self.__streams_queue.empty():
            unit_id = self.__thread_pool.get()
            self._events_list[unit_id] = self.__streams_queue.get_nowait().duplicate()  # stream of input data
            self.__start_list[unit_id].add_item(self.__start_queue.get_nowait())

        # finished to divide the data
        for i in range(0, self._units_number - 1):
            self.__start_list[i].close()

        while self._mutex.qsize() < self._units_number - 1:
            time.sleep(0.01)
        self.__matches_handler.close()

    def _eval_thread(self, thread_id: int, data_formatter: DataFormatter):

        for _ in self.__start_list[thread_id]:
            self._eval_trees[thread_id].eval(self._events_list[thread_id], self.__matches_handler, data_formatter, False)
            self._eval_trees[thread_id] = EvaluationMechanismFactory.build_eval_mechanism(self._eval_mechanism_params, self._patterns)
            self.__thread_pool.put(thread_id)

        self._mutex.put(1)

    """
        check if the match is in an section where it  suspected of duplication 
    """
    def __check_duplicated_matches(self, match):
        while self.__algorithm_start_time == -1:
            pass
        delta = self.__time_slot - self.__shared_time
        if len(match.events) > 1:
            first = match.first_timestamp - self.__algorithm_start_time
            last = match.last_timestamp - self.__algorithm_start_time
            if math.floor(first / delta) != math.floor(last / delta):
                return False

        for event in match.events:
            index = math.floor((event.timestamp - self.__algorithm_start_time) / delta)
            end_of_share_time = self.__algorithm_start_time + index * self.__time_slot
            if end_of_share_time < event.timestamp:  # do not need to check if duplicated
                return False

        return True

    """
        remove duplicated matches and send the matches to the output stream
    """
    def __make_output_matches_stream(self):

        for match in self.__matches_handler:
            is_duplicated = self.__check_duplicated_matches(match)
            if is_duplicated:
                if match.__str__() in self.__duplicated_matches:
                    self.__duplicated_matches.remove(match.__str__())
                else:
                    self._matches.add_item(match)
                    self.__duplicated_matches.append(match.__str__())
            else:
                self._matches.add_item(match)
        self._matches.close()

    def eval_algorithm(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):

        super().eval_algorithm(events, matches, data_formatter)
        self.__make_output_matches_stream()


class HyperCubeAlgorithm(DataParallelAlgorithm, ABC):
    """
           A class for data parallel evaluation HyperCube algorithm
    """
    def __init__(self, threads_num, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters, platform, attributes_dict: dict):
        super().__init__(threads_num, patterns, eval_mechanism_params, platform)
        self.attributes_dict = attributes_dict
        self.types = []
        for pattern in patterns:
            self.types.extend(list(pattern.get_all_event_types_with_duplicates()))
        self.groups_num = math.ceil((self._units_number - 1) ** (1 / len(self.types)))
        self.matches_handler = Stream()
        self.finished_threads = [0]
        self.mutex = Lock()

    def eval_algorithm(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        super().eval_algorithm(events, matches, data_formatter)
        check_duplicated = list()

        for match in self.matches_handler:
            if match.__str__() not in check_duplicated:
                self._matches.add_item(match)
                check_duplicated.append(match.__str__())
        self._matches.close()

    def _stream_divide(self):
        for event_raw in self._events:
            event = Event(event_raw, self._data_formatter)
            event_type = event.type
            if event_type not in self.types:
                continue
            if event_type not in self.attributes_dict.keys():
                raise Exception("%s has no matching attribute" % event_type)
            for index in range(len(self.attributes_dict[event_type])):
                event_attribute = self.attributes_dict[event_type][index]
                attribute_val = event.payload[event_attribute]
                if not isinstance(attribute_val, (int, float)):
                    raise Exception(
                        "Attribute %s has no numeric value" % event_attribute)
                group_index = int(attribute_val % self.groups_num)
                type_index = self._finding_type_index_considering_duplications(
                    index, event_type)
                leg_size = self.groups_num ** type_index
                new_start = group_index * leg_size
                jump = leg_size * (self.groups_num - 1) + 1
                j = new_start
                while j < (self._units_number - 1):
                    self._events_list[j].add_item(event_raw)
                    leg_size -= 1
                    if leg_size == 0:
                        j += jump
                        leg_size = self.groups_num ** type_index
                    else:
                        j += 1
        for stream in self._events_list:
            stream.close()
        while self.finished_threads[0] != (self._units_number - 1):
            time.sleep(0.0001)
        self.matches_handler.close()

    def _eval_thread(self, thread_id: int, data_formatter: DataFormatter):
        self._eval_trees[thread_id].eval(self._events_list[thread_id], self.matches_handler, data_formatter, False, self.finished_threads, self.mutex)

    def _finding_type_index_considering_duplications(self, index_among_type, event_type):
        count = 0
        i = 0
        while list(self.attributes_dict.keys())[i] != event_type:
            key = list(self.attributes_dict.keys())[i]
            count += len(self.attributes_dict[key])
            i += 1
        count += index_among_type
        return count


