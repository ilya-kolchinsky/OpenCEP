from tree.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism
from parallerization.ParallelExecutionFramework import ParallelExecutionFramework
from stream.Stream import OutputStream, Stream
from base.DataFormatter import DataFormatter

from parallerization.tree_implemintation.PatternMatchWithUnarySource import PatternMatchWithUnarySource

import time
import threading
from queue import Queue


class ParallelTreeEval(ParallelExecutionFramework):

    def __init__(self, tree_based_eval: TreeBasedEvaluationMechanism, has_leafs: bool, is_main_root: bool,
                 data_formatter: DataFormatter = None):
        super().__init__(tree_based_eval, data_formatter)

        self.has_leafs = has_leafs
        self.is_main_root = is_main_root
        self.children = []

        self.queue = Queue()
        self.finished = threading.Event()
        self.finished.clear()
        self.keep_running = threading.Event()
        self.keep_running.set()

        self.thread = threading.Thread(target=self.run_eval, args=())

    def set_children(self, children: ParallelExecutionFramework):
        self.children = children

    def add_child(self, child: ParallelExecutionFramework):
        self.children.append(child)

    def get_thread(self):
        return self.thread

    def activate(self):
        self.thread.start()

    def stop(self):
        self.keep_running.clear()

    def process_event(self, event_stream: Stream):
        self.queue.put(event_stream)

    def wait_until_finish(self):
        self.finished.wait()

    def join(self):
        self.thread.join()

    def run_eval(self):
        if self.has_leafs:
            self.run_eval_with_leafs()
        else:
            for child in self.children:
                child.wait_until_finish()
            self.run_eval_without_leafs()

        self.finished.set()

    def run_eval_with_leafs(self):
        time.sleep(30)                  # TODO : check

        while self.keep_running.is_set() or not self.queue.qsize() == 0:
            if not self.queue.qsize() == 0:
                event = self.queue.get()
                self.evaluation_mechanism.eval(event, self.pattern_matches, self.data_formatter)

        # while not self.queue.qsize() == 0:
        #     event = self.queue.get()
        #     self.evaluation_mechanism.eval(event, self.pattern_matches, self.data_formatter)

    def run_eval_without_leafs(self):
        unary_children = self.get_unary_children()
        partial_matches_list = []

        unary_children_index = 0
        for unary_child in unary_children:
            partial_matches = unary_child.get_our_matches()
            for match in partial_matches:
                new_match_object = PatternMatchWithUnarySource(match, unary_children_index)
                partial_matches_list.append(new_match_object)
            unary_children_index += 1

        partial_matches_list = sorted(partial_matches_list, key=lambda x: x.get_pattern_match_timestamp())
        for pm in partial_matches_list:
            unary_child = unary_children[pm.unary_index]
            unary_child.handle_event(pm.pattern_match)

        if self.is_main_root:
            for match in self.evaluation_mechanism.get_tree().get_root().get_our_matches():
                self.pattern_matches.add_item(match)

            for match in self.evaluation_mechanism.get_tree().get_last_matches():
                self.pattern_matches.add_item(match)

            self.pattern_matches.close()

    def get_unary_children(self):
        unary_children = []
        for child in self.children:
            unary_children.append(child.get_evaluation_mechanism().get_root())
        return unary_children
