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

        self.stopped = False

        self.thread = threading.Thread(target=self.run_eval, args=())

    def set_children(self, children):
        self.children = children

    def get_thread(self):
        return self.thread

    def activate(self):
        self.thread.start()
        print("activating thread " + str(self.thread.ident))

    def stop(self):
        self.keep_running.clear()

    def process_event(self, event):
        # print("pushing event to thread " + str(self.thread.ident))
        self.queue.put(event)

    def wait_till_finish(self):
        self.finished.wait()

    def join(self):
        self.thread.join()
    #     self.stopped = True

    # def join(self):
    #     self.queue.join()

    def get_stopped(self):
        return self.stopped

    def run_eval(self): # thread
        try:
            print("run_eval of thread " + str(self.thread.ident))
            if self.has_leafs:
                print("run_eval_with_leafs of thread " + str(self.thread.ident))
                try:
                    self.run_eval_with_leafs()
                except:
                    print("**********************")
                    raise Exception("1")
            else:
                print("run_eval_without_leafs of thread " + str(self.thread.ident))
                self.run_eval_without_leafs()

            print("almost finished running thread " + str(self.thread.ident))
            self.finished.set()
            print("finished running thread " + str(self.thread.ident))
        except:
            print("---------------------")
            print("10")
            raise Exception("10")

    def run_eval_with_leafs(self):
        print(" called running run_eval_with_leafs on thread " + str(self.thread.ident) + " : " + str(self.keep_running.is_set()) + " " + str(self.queue._qsize()))

        while self.keep_running.is_set():
            # print(str(self.thread.ident) + " is running " + str(self.keep_running.is_set()) + " " + str(self.queue.qsize()))
            try:
                event = self.queue.get()
                self.queue.task_done()
                # print("1 calling eval on thread " + str(self.thread.ident))
                self.evaluation_mechanism.eval(event, self.pattern_matches, self.data_formatter)
            except:
                pass

        while not self.queue.empty():
            # print(str(self.thread.ident) + " is running " + str(self.keep_running.is_set()) + " " + str(self.queue.qsize()))
            try:
                event = self.queue.get()
                self.queue.task_done()
                # print("2 calling eval on thread " + str(self.thread.ident))
                self.evaluation_mechanism.eval(event, self.pattern_matches, self.data_formatter)
            except:
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
