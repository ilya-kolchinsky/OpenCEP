from tree.PatternMatchStorage import TreeStorageParameters
from tree.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism
from parallerization.tree_implemintation.ParallelUnaryNode import ParallelUnaryNode
from parallerization.ParallelExecutionFramework import ParallelExecutionFramework
from base.PatternMatch import PatternMatch
from stream.Stream import InputStream, OutputStream
from base.DataFormatter import DataFormatter

from parallerization.tree_implemintation.PatternMatchWithUnarySource import PatternMatchWithUnarySource

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

    # def get_stopped(self):
    #     return self.stopped

    def run_eval(self): # thread
        try:
            print("run_eval of thread " + str(self.thread.ident))
            if self.has_leafs:
                try:
                    self.run_eval_with_leafs()
                except:
                    print("**********************")
                    raise Exception("1")
            else:
                time.sleep(10)
                for child in self.children:
                    print(str(self.thread.ident) + " waiting for child " + str(child.get_thread().ident) + " to finish ")
                    child.wait_till_finish()
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
        counter = 0
        time.sleep(20)
        while self.keep_running.is_set():
            # print(str(self.thread.ident) + " is running " + str(self.keep_running.is_set()) + " " + str(self.queue.qsize()))
            try:
                if not self.queue._qsize() == 0:
                    event = self.queue.get()
                    # self.queue.task_done()
                    counter += 1
                    if counter % 10000 == 0:
                        print(str(self.thread.ident) + " 1: counter  =  " + str(counter))
                    # print("1 calling eval on thread " + str(self.thread.ident))
                    self.evaluation_mechanism.eval(event, self.pattern_matches, self.data_formatter)
            except:
                raise Exception("---")

        while not self.queue._qsize() == 0:
            # print(str(self.thread.ident) + " is running " + str(self.keep_running.is_set()) + " " + str(self.queue.qsize()))
            try:
                event = self.queue.get()
                # self.queue.task_done()
                counter += 1
                if counter % 10000 == 0:
                    print(str(self.thread.ident) + " 2: counter  =  " + str(counter))
                # print("2 calling eval on thread " + str(self.thread.ident))
                self.evaluation_mechanism.eval(event, self.pattern_matches, self.data_formatter)
            except:
                pass

    def run_eval_without_leafs(self):
        print("run_eval_without_leafs of thread " + str(self.thread.ident))
        unary_children = self.get_unary_children()

        partial_matches_list = []
        i = 0
        for unary_child in unary_children:
            partial_matches = unary_child.get_partial_matches()
            for match in partial_matches:
                new_match_object = PatternMatchWithUnarySource(match, i)
                partial_matches_list.append(new_match_object)
            i += 1
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