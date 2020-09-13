from base import Pattern
from evaluation.PartialMatchStorage import TreeStorageParameters
from evaluation.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism, UnaryNode, Node
from parallerization.ParallelExecutionFramework import ParallelExecutionFramework
from datetime import timedelta
from typing import Tuple, Dict, List
from base.PatternStructure import QItem
from base.PatternMatch import PatternMatch
from misc.IOUtils import Stream

from datetime import timedelta, datetime
import time
import threading


class ParallelUnaryNode(UnaryNode):
    def __init__(self, sliding_window: timedelta, parent: Node = None,
                 event_defs: List[Tuple[int, QItem]] = None, child: Node = None):
        super().__init__(sliding_window, parent, event_defs, child)
        self._is_done = False

    def get_done(self):
        return self._is_done

    def light_is_done(self):
        self._is_done = True

    def get_child(self):
        return self._child

    def get_leaves(self):
        return [self]

    def clean_expired_partial_matches(self, last_timestamp: datetime):
        return


class UnaryParallelTreeEval(ParallelExecutionFramework):

    def __init__(self, tree_based_eval: TreeBasedEvaluationMechanism, has_leafs: bool):
        if type(tree_based_eval) is not TreeBasedEvaluationMechanism:
            raise Exception()
        super().__init__(tree_based_eval)
        self._has_leafs = has_leafs
        self.add_unary_root()

    def add_unary_root(self):
        root = self._evaluation_mechanism.get_tree().get_root()
        unary_node = ParallelUnaryNode(root._sliding_window, child=root)
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
        pattern_matches.close()

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
            if type(children[i]) is not ParallelUnaryNode:
                children.remove(children[i])
        return children

    def all_children_done(self):
        children = self.get_children()
        if children is None:
            return True
        for child in children:
            child_done = child._is_done.try_to_lock()#TODO check python lock
            if not child_done:
                child._is_done.unlock()
                return False
            else:
                child._is_done.unlock()
        return True

    def get_partial_matches_from_children(self, events: Stream):
        children = self.get_children()
        if children is None:#if self has no unaryParallelNode children, it means that it has only leaves and then needs the source input
            return events
        aggregated_events = Stream()
        for child in children:
            if type(child) is not ParallelUnaryNode:
                raise Exception()
            #LOCK
            for match in child._partial_matches:
                aggregated_events.add_item(match)
            #UNLOCK
        return aggregated_events

    def eval_util(self, events: Stream, matches: Stream, is_async=False, file_path=None, time_limit: int = None):
        self._evaluation_mechanism.eval(events, matches, is_async, file_path, time_limit)

    def modified_eval(self, events: Stream, matches: Stream, is_async=False, file_path=None, time_limit: int = None):
        if self.all_children_done():
            aggregated_events = self.get_partial_matches_from_children(events)
            self.eval_util(aggregated_events, matches, is_async, file_path, time_limit)  # original eval
            self._evaluation_mechanism.get_tree().light_is_done()
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
