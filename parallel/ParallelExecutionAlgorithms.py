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

from datetime import datetime

def _eval_thread(tree, events: Stream, matches: Stream, final_matches: DataParallelOutputStream, data_formatter: DataFormatter):
    print("&")
    tree.eval(events, matches, data_formatter)

    try:
        stream_iter = iter(matches)
        item = next(stream_iter)
        while item:  # adding all items expect the last one to the output stream
            final_matches.add_item(item)
            item = next(stream_iter)
    except StopIteration:
        pass





class DataParallelAlgorithm(ABC):
    """
        An abstract base class for all  data parallel evaluation algorithms.
        """

    def __init__(self, numthreads, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters ):

        self._numThreads = numthreads
        self._threads = []
        self._trees = []
        self._events = None
        self._stream_thread = None

        for i in range(0, self._numThreads):
            if isinstance(patterns, Pattern):
                patterns = [patterns]
            if len(patterns) > 1:
                self._trees.append(EvaluationMechanismFactory.build_multi_pattern_eval_mechanism(
                    eval_mechanism_params,
                    patterns))
            else:
                self._trees.append(EvaluationMechanismFactory.build_single_pattern_eval_mechanism(
                    eval_mechanism_params,
                    patterns[0]))

        self._events_list = []
        for i in range(numthreads):
            self._events_list.append(Stream())
        #################for the tests
        self._matches_list = []

        for i in range(numthreads):
            self._matches_list.append(Stream())



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



    def eval_algorithm(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter, platform):

        buffer_matches = DataParallelOutputStream()
        self._events = events
        self._stream_thread = platform.create_parallel_execution_unit(unit_id= self._numThreads, callback_function= self._stream_divide)
        self._stream_thread.start()

        for i in range(self._numThreads):
            t = platform.create_parallel_execution_unit(unit_id=i, callback_function=_eval_thread, tree=self._trees[i],
                                                              events=self._events_list[i], matches=self._matches_list[i], final_matches=buffer_matches, data_formatter=data_formatter)
            self._threads.append(t)

        for t in self._threads:
            t.start()
            t.wait()
        self._stream_thread.wait()

        ######################## for the test
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


class Algorithm3(DataParallelAlgorithm):
    def eval_algorithm(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        raise NotImplementedError







