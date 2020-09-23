from base import Pattern
from tree.PatternMatchStorage import TreeStorageParameters
from tree.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism
from parallerization.ParallelUnaryNode import ParallelUnaryNode
from parallerization.ParallelExecutionFramework import ParallelExecutionFramework
from base.PatternMatch import PatternMatch
from stream.Stream import InputStream, OutputStream

import time
import threading


class ParallelTreeEval(ParallelExecutionFramework): # returns from split: List[ParallelTreeEval]

    def __init__(self, tree_based_eval: TreeBasedEvaluationMechanism, has_leafs: bool, is_main_root: bool,
                 is_multithreaded : bool = True):
        if type(tree_based_eval) is not TreeBasedEvaluationMechanism:
            raise Exception()
        super().__init__(tree_based_eval) # tree_based_eval is unique for thread
        self._unary_root = tree_based_eval.get_tree().get_root() # unaryNode
        self._all_leaves_are_original_leaves = has_leafs
        self._is_done = False
        self._thread = None
        self._unary_children = [] # list of type ParallelTreeEval
        self._is_main_root = is_main_root
        self._all_children_done_flag = False
        self._is_multithreaded = is_multithreaded
        self.add_unary_root() #TODO: check if needed

    def add_unary_children(self, children_eval):
        self._unary_children.append(children_eval)

    def add_unary_root(self):
        root = self._evaluation_mechanism.get_tree().get_root()
        if type(root) is ParallelUnaryNode:
            return
        unary_node = ParallelUnaryNode(True, root._sliding_window, child=root)
        unary_node.set_subtree(root)
        root.set_parent(unary_node)
        self._evaluation_mechanism.set_root(unary_node)
        storageparams = TreeStorageParameters(True)
        self._evaluation_mechanism.get_tree().get_root().create_storage_unit(storageparams)

        self.root = self._evaluation_mechanism.get_tree().get_root() # unaryNode


    def get_done(self):
        return self._is_done

    def light_is_done(self):
        self._is_done = True

    def stop(self):
        raise NotImplementedError()

    def get_data(self):
        raise NotImplementedError()

    def get_final_results(self, pattern_matches: OutputStream):
        for match in self._evaluation_mechanism.get_tree().get_matches():
            pattern_matches.add_item(PatternMatch(match))
        #pattern_matches.close()

    def wait_till_finish(self):
        root = self._evaluation_mechanism.get_tree().get_root()

        if type(root) is not ParallelUnaryNode:
            # tree not built properly
            raise Exception()

        while not root.get_done():
            time.sleep(0.5)

    def get__unary_children(self):
        return self._unary_children

    def get_children_old(self):
        # returns a list of all the UnaryParallelNodes that are directly connected to self.tree_based
        root = self._evaluation_mechanism.get_tree().get_root()
        if type(root) is not ParallelUnaryNode:
            # tree not built properly
            raise Exception()
        children = self._evaluation_mechanism.get_tree().get_leaves()
        for i in range(len(children)):
            if children[i] == root: # TODO remove
                children.remove(children[i])
            if type(children[i]) is not ParallelUnaryNode:
                children.remove(children[i])
        return children

    def get_partial_matches_from__unary_children(self, events: InputStream):
        unary_children = self.get__unary_children()
        if len(unary_children) == 0:#if self has no unaryParallelNode children, it means that it has only leaves and then needs the source input
            return events
        aggregated_events = InputStream()

        for child in unary_children:
            if type(child) is not ParallelUnaryNode:
                raise Exception()

            for match in child._partial_matches:
                aggregated_events.add_item(match)

        return aggregated_events

    def eval_util(self, events: InputStream, matches: OutputStream, data_formatter):
        input_stream = None

        while not self._is_done: # TODO: change implementation to waiting to synchronized var
            if self._all_children_done_flag:
                if self._all_leaves_are_original_leaves:
                    input_stream = events
                else:
                    input_stream = self.get_partial_matches_from_unary_children()

                # input_stream.close()

                if self._all_leaves_are_original_leaves:
                    self.eval_util_with_original_leaves(input_stream, matches, data_formatter)
                else:
                    self.eval_util_with_no_original_leaves(input_stream, matches)

                self.light_is_done()
            else:
                time.sleep(0.5) # TODO: change to wait for synchronized var

    def run_eval(self, event_stream, pattern_matches,data_formatter):  # THREAD running
        #print('Running thread with id', threading.get_ident())

        self.eval_util(event_stream, pattern_matches, data_formatter)

        #print('Finished running thread with id', threading.get_ident())
        return

    def eval(self, event_stream, pattern_matches, data_formatter):
        #print('Running MAIN thread with id', threading.get_ident())
        if self._is_multithreaded:
            self.thread = threading.Thread(target=self.run_eval, args=(event_stream, pattern_matches, data_formatter,))
            self.thread.start()
        else:
            self.run_eval(event_stream, pattern_matches, data_formatter)

    def all_children_done(self):
        if self.all_children_done_flag == False:
            lock = threading.Lock()
            children = self.get_unary_children()
            if len(children) == 0:
                return True
            for child in children:
                lock.acquire()
                child_done = child.get_done()
                if not child_done:
                    lock.release()
                    #child._is_done.release()
                    return False
                else:
                    lock.release()
                    #child._is_done.unlock()
                    pass
            self.all_children_done_flag = True
            return True
        else:
            return True

    def eval_util_with_original_leaves(self, events: InputStream, matches: OutputStream, data_formatter):
        self._evaluation_mechanism.eval(events, matches,data_formatter)

    def eval_util_with_no_original_leaves(self, partial_matches: InputStream, matches: OutputStream):

        unary_children = self.get_my_unary_children()

        input_stream = []
        for child in unary_children:
            partial_matches_list = child.get_partial_matches()
            input_stream += input_stream

        input_stream_sotrted = sort(input_stream) #TODO:

        for pm in input_stream_sotrted:
            child = check_which_leaf(pm) #TODO:
            child.handle_event(pm)

        if self.is_main_root:
            for match in self._evaluation_mechanism.get_tree().get_matches():  # for all the matches in the root
                matches.add_item(PatternMatch(match))

        for match in self._evaluation_mechanism.get_tree().get_last_matches():
            matches.add_item(PatternMatch(match))
        matches.close()
