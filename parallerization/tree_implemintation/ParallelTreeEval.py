from tree.PatternMatchStorage import TreeStorageParameters
from tree.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism
from parallerization.tree_implemintation.ParallelUnaryNode import ParallelUnaryNode
from parallerization.ParallelExecutionFramework import ParallelExecutionFramework
from base.PatternMatch import PatternMatch
from stream.Stream import InputStream, OutputStream
from base.DataFormatter import DataFormatter

import time
import threading
from queue import Queue


class ParallelTreeEval(ParallelExecutionFramework): # returns from split: List[ParallelTreeEval]

    def __init__(self, tree_based_eval: TreeBasedEvaluationMechanism, has_leafs: bool, is_main_root: bool,
                 data_formatter: DataFormatter = None, children: list = None):
        super().__init__(tree_based_eval, data_formatter)

        self.has_leafs = has_leafs
        self.is_main_root = is_main_root
        self.children = children

        self.queue = Queue()
        self.finished = threading.Event()
        self.finished.clear()
        self.keep_running = threading.Event()
        self.keep_running.set()

        self.thread = threading.Thread(target=self.run_eval, args=())

    def set_children(self, children):
        self.children = children

    def activate(self):
        self.thread.start()

    def stop(self):
        self.keep_running.clear()

    def process_event(self, event):
        self.queue.put(event)

    def wait_till_finish(self):
        self.finished.wait()

    def run_eval(self): # thread
        if self.has_leafs:
            self.run_eval_with_leafs()
        else:
            self.run_eval_without_leafs()

        self.finished.set()

    def run_eval_with_leafs(self):
        while self.keep_running.is_set() or not self.queue.empty():
            if not self.queue.empty():
                event = self.queue.get()
                self.evaluation_mechanism.eval(event, self.pattern_matches, self.data_formatter)
            else:
                pass

    def run_eval_without_leafs(self):
        unary_children = self.get_unary_children()

        while self.keep_running.is_set():
            if not self.all_unary_children_finished():
                pass
            else:
                partial_matches_list = []
                for child in unary_children:
                    partial_matches = child.get_partial_matches()
                    partial_matches_list += partial_matches

                partial_matches_list = self.sort_partial_matches_list(partial_matches_list)

                for pm in partial_matches_list:
                    child = self.get_destination(pm)
                    child.handle_event(pm)

                if self.is_main_root:
                    for match in self.evaluation_mechanism.get_tree().get_matches():
                        self.pattern_matches.add_item(PatternMatch(match))

                for match in self.evaluation_mechanism.get_tree().get_last_matches():
                    self.pattern_matches.add_item(PatternMatch(match))

                self.pattern_matches.close()

    def get_unary_children(self):
        raise NotImplementedError

    def all_unary_children_finished(self):
        raise NotImplementedError

    def sort_partial_matches_list(self, partial_matches_list):
        raise NotImplementedError

    def get_destination(self, pm):
        raise NotImplementedError()

    def get_final_results(self, pattern_matches: OutputStream):
        for match in self.evaluation_mechanism.get_tree().get_matches():
            if match is not None:
                pattern_matches.add_item(PatternMatch(match))
