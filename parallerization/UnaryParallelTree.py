from base import Pattern
from evaluation.PartialMatchStorage import TreeStorageParameters
from evaluation.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism, UnaryNode, Node
from parallerization.ParallelExecutionFramework import ParallelExecutionFramework
from typing import Tuple, Dict, List
from base.PatternStructure import QItem
from base.PatternMatch import PatternMatch
from misc.IOUtils import Stream

from datetime import timedelta, datetime
import time
import threading

class ParallelUnaryNode(UnaryNode):
    def __init__(self, is_root: bool, sliding_window: timedelta, parent: Node = None,
                 event_defs: List[Tuple[int, QItem]] = None, child: Node = None):
        super().__init__(sliding_window, parent, event_defs, child)
        self._is_done = False
        self._is_root = is_root

    def get_done(self):
        return self._is_done

    def light_is_done(self):
        self._is_done = True

    def get_child(self):
        return self._child

    def get_leaves(self):
        if self._is_root:
            return self._child.get_leaves()
        else:
            return [self]

    def clean_expired_partial_matches(self, last_timestamp: datetime):
        return

    def handle_new_partial_match(self, partial_match_source: Node):
        new_partial_match = partial_match_source.get_last_unhandled_partial_match()
        super()._add_partial_match(new_partial_match)


class UnaryParallelTreeEval(ParallelExecutionFramework):

    def __init__(self, tree_based_eval: TreeBasedEvaluationMechanism, has_leafs: bool):
        if type(tree_based_eval) is not TreeBasedEvaluationMechanism:
            raise Exception()
        super().__init__(tree_based_eval)
        self._has_leafs = has_leafs
        self.add_unary_root()
        storageparams = TreeStorageParameters(True)
        self._evaluation_mechanism.get_tree().get_root().create_storage_unit(storageparams)

    def add_unary_root(self):
        root = self._evaluation_mechanism.get_tree().get_root()
        unary_node = ParallelUnaryNode(True, root._sliding_window, child=root)
        unary_node.set_subtree(root)
        root.set_parent(unary_node)
        self._evaluation_mechanism.set_root(unary_node)

    def stop(self):
        raise NotImplementedError()

    def get_data(self):
        raise NotImplementedError()

    def get_final_results(self, pattern_matches: Stream):
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

    def get_children(self):
        # returns a list of all the UnaryParallelNodes that are directly connected to self.tree_based
        root = self._evaluation_mechanism.get_tree().get_root()
        if type(root) is not ParallelUnaryNode:
            # tree not built properly
            raise Exception()
        children = self._evaluation_mechanism.get_tree().get_leaves()
        for i in range(len(children)):
            if children[i] == root:#TODO remove
                children.remove(children[i])
            elif type(children[i]) is not ParallelUnaryNode:
                children.remove(children[i])
        return children

    def get_partial_matches_from_children(self, events: Stream):
        children = self.get_children()
        if len(children) == 0:#if self has no unaryParallelNode children, it means that it has only leaves and then needs the source input
            return events
        aggregated_events = Stream()

        lock = threading.Lock()

        for child in children:
            if type(child) is not ParallelUnaryNode:
                raise Exception()
            #LOCK
            lock.acquire()
            for match in child._partial_matches:
                aggregated_events.add_item(match)
            #UNLOCK
            lock.acquire()

        return aggregated_events

    def eval_util(self, events: Stream, matches: Stream, is_async=False, file_path=None, time_limit: int = None):
        self._evaluation_mechanism.eval(events, matches, is_async, file_path, time_limit)

    def modified_eval(self, events: Stream, matches: Stream, is_async=False, file_path=None, time_limit: int = None):
        if self.all_children_done():
            aggregated_events = self.get_partial_matches_from_children(events)
            aggregated_events.close()
            self.eval_util(aggregated_events, matches, is_async, file_path, time_limit)  # original eval
            self._evaluation_mechanism.get_tree().get_root().light_is_done()
        else:
            time.sleep(0.5)

    def run_eval(self, event_stream, pattern_matches, is_async, file_path, time_limit):  # thread running
        root = self._evaluation_mechanism.get_tree().get_root()
        if type(root) is not ParallelUnaryNode:
            # tree not built properly
            raise Exception()
        while not root.get_done():
            self.modified_eval(event_stream, pattern_matches, is_async, file_path, time_limit)

    def eval(self, event_stream, pattern_matches, is_async=False, file_path=None, time_limit: int = None):
        thread = threading.Thread(target=self.run_eval, args=(event_stream, pattern_matches, is_async,
                                                              file_path, time_limit,))
        thread.start()
        #self.run_eval(event_stream, pattern_matches, is_async, file_path, time_limit)

    def all_children_done(self):
        lock = threading.Lock()
        children = self.get_children()
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
        return True
