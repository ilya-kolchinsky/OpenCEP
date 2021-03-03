"""
 Data parallel RIP algorithms
"""
from abc import ABC
from stream.DataParallelStream import *
from parallel.ParallelExecutionAlgorithms import DataParallelAlgorithm
import math
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import \
    EvaluationMechanismParameters, EvaluationMechanismFactory
from base.DataFormatter import DataFormatter
from base.PatternMatch import *


class RIPAlgorithm(DataParallelAlgorithm, ABC):
    """
        A class for data parallel evaluation RIP algorithm
    """

    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters,
                 platform, multiple):
        super().__init__(units_number - 1, patterns, eval_mechanism_params,
                         platform)
        self.__eval_mechanism_params = eval_mechanism_params
        self.__matches_handler = Stream()

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
                             range(self._units_number)]
        self.__start_queue = Queue()
        self.__streams_queue = Queue()
        self.__thread_pool = Queue()
        self.__matches_unit = self._platform.create_parallel_execution_unit(
            unit_id=units_number - 1,
            callback_function=self.__make_output_matches_stream)

        for i in range(self._units_number):
            self.__thread_pool.put(i)

        self._mutex = Queue()
        self.__duplicated_matches = list()

    def _stream_divide(self):
        """
               Divide the input stream into calculation units according to events times
        """

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
        for i in range(0, self._units_number):
            self.__start_list[i].close()

    def _eval_unit(self, thread_id: int, data_formatter: DataFormatter):

        for _ in self.__start_list[thread_id]:
            self._eval_trees[thread_id].eval(self._events_list[thread_id], self.__matches_handler, data_formatter, False)
            self._eval_trees[thread_id] = EvaluationMechanismFactory.build_eval_mechanism(self.__eval_mechanism_params, self._patterns)
            self.__thread_pool.put(thread_id)

    def __check_duplicated_matches(self, match):
        """
                check if the match is in an section where it  suspected of duplication
        """
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

    def __make_output_matches_stream(self):
        """
                remove duplicated matches and send the matches to the output stream
        """

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
        self.__matches_unit.start()
        self._stream_divide()
        for t in self._units:
            t.wait()

        self.__matches_handler.close()
        self.__matches_unit.wait()
