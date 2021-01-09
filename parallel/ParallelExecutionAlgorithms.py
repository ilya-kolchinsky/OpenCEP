"""
 Data parallel algoritms
"""
from abc import ABC
from stream.Stream import InputStream, OutputStream
from stream.DataParallelStream import *
from stream.Stream import Stream

from typing import List
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters, EvaluationMechanismFactory
from base.DataFormatter import DataFormatter
from queue import Queue
from datetime import datetime
import threading


def _make_tree(patterns: Pattern or List[Pattern],
               eval_mechanism_params: EvaluationMechanismParameters):
    if isinstance(patterns, Pattern):
        patterns = [patterns]
    if len(patterns) > 1:
        tree = EvaluationMechanismFactory.build_multi_pattern_eval_mechanism(
            eval_mechanism_params,
            patterns)
    else:
        tree = EvaluationMechanismFactory.build_single_pattern_eval_mechanism(
            eval_mechanism_params,
            patterns[0])

    return tree


class DataParallelAlgorithm(ABC):
    """
        An abstract base class for all  data parallel evaluation algorithms.
        """

    def __init__(self, numthreads, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters, platform):
        self._platform = platform
        self._numThreads = numthreads
        self._threads = []
        self._trees = []
        self._events = None
        self._events_list = []
        self._stream_thread = platform.create_parallel_execution_unit(unit_id=self._numThreads - 1, callback_function=self._stream_divide)
        self._still_working = True
        self._matches = None
        self._paterrns = patterns


        """ 
        self._matches_list = []

        for i in range(numthreads):
            self._matches_list.append(Stream())
        """

    def eval_algorithm(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        raise NotImplementedError()


class Algorithm1(DataParallelAlgorithm):
    def eval_algorithm(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        raise NotImplementedError


class Algorithm2(DataParallelAlgorithm):

    def _stream_divide(self):

        ####for the tests
        start = datetime.now()
        try:
            stream_iter = iter(self._events)
            item = next(stream_iter)
            while item:  # adding all items expect the last one to the output stream
                for i in range(self._numThreads):
                    self._events_list[i].add_item(item)
                item = next(stream_iter)
        except StopIteration:
            for i in range(self._numThreads):
                self._events_list[i].add_item(None)
        finally:
            self._events.close()

        print("time: ", (datetime.now() - start).total_seconds())

    def _eval_thread(self, thread_id: int, data_formatter: DataFormatter):



        for begin_time in  self._times[thread_id]:
            time1, time2 #todo: calculate the shared times
            self._trees[thread_id].eval_parallel(self._events_list, self._matches_buffer, data_formatter, time1, time2)
            self._trees[thread_id] = _make_tree(self._patterens)
            self._pool.append(thread_id)  # todo: change to the name linor given to the threads queue
        
    def _eval_test(self, thread_id, output):
       while(1):
        for item in self._events_list[thread_id]:
            output.add_item(item)

    def _match_to_output(self):
        duplicated = set()
        for match, flag in self._buffer:
            if flag:
                if match in duplicated:
                    duplicated.remove(match)
                else:
                    self._matches.add_item(match)
                    duplicated.add(match)
            else:
                self._matches.add_item(match)

    def __init__(self, numthreads, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters, platform):

        super().__init__(numthreads, patterns,eval_mechanism_params, platform)

        for i in range(0, self._numThreads-2):
            self._trees.append(_make_tree(patterns, eval_mechanism_params))
            self._events_list.append(Stream())
            self._output_handler = platform.create_parallel_execution_unit(unit_id=numthreads-2, callback_function=self._match_to_output)
            self._matches_buffer = Stream()
            #TODO: check with linor
            self._init_time = None
            self._epoch_time = None
            self._shared_time = None
            self._times = []
            for _ in self._numThreads-2:
                self._times.append(Queue())
                self._pool.append(i)  # todo: change to the name linor given to the threads queue


    def eval_algorithm(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):

        self._events = events
        self._matches = matches
        self._stream_thread.start()

        for i in range(self._numThreads -2):
            t = self._platform.create_parallel_execution_unit(unit_id=i, callback_function=self._eval_test, thread_id=i, output=matches)
            #t = self._platform.create_parallel_execution_unit(unit_id=i, callback_function=self._eval_thread, thread_id=i, data_formatter=data_formatter)
            self._threads.append(t)
            t.start()


        self._output_handler.start()

        for t in self._threads:
            t.wait()

        self._output_handler.wait()
        ######################## for the test
        """
        buffer_matches.close()
        try:
            stream_iter = iter(buffer_matches)
            item = next(stream_iter)
            while item:  # adding all items expect the last one to the output stream
                matches.add_item(item)
                item = next(stream_iter)
        except StopIteration:
            pass

        matches.close()
        """



class Algorithm3(DataParallelAlgorithm):
    def eval_algorithm(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        raise NotImplementedError
