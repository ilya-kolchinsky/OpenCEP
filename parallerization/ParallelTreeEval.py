from base import Pattern
from evaluation.PartialMatchStorage import TreeStorageParameters
from evaluation.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism, UnaryNode, Node
from parallerization import ParallelUnaryNode
from parallerization.ParallelExecutionFramework import ParallelExecutionFramework
from typing import Tuple, Dict, List
from base.PatternStructure import QItem
from base.PatternMatch import PatternMatch
from misc.IOUtils import Stream

from datetime import timedelta, datetime
import time
import threading



class ParallelTreeEval(ParallelExecutionFramework): # returns from split: List[ParallelTreeEval]

    def __init__(self, tree_based_eval: TreeBasedEvaluationMechanism, has_leafs: bool, is_main_root: bool):
        if type(tree_based_eval) is not TreeBasedEvaluationMechanism:
            raise Exception()
        super().__init__(tree_based_eval) # tree_based_eval is unique for thread
        self.root = tree_based_eval.get_tree().get_root() # unaryNode
        self._has_leafs = has_leafs
        self._is_done = False
        self.add_unary_root()
        self.thread = None
        self.children = [] # list of type ParallelTreeEval
        self.is_main_root = is_main_root
        self.all_children_done_flag = False

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

    def get_done(self):
        return self._is_done

    def light_is_done(self):
        self._is_done = True

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
        return self.children

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

    def get_partial_matches_from_children(self, events: Stream):
        children = self.get_children()
        if len(children) == 0:#if self has no unaryParallelNode children, it means that it has only leaves and then needs the source input
            return events
        aggregated_events = Stream()

        for child in children:
            if type(child) is not ParallelUnaryNode:
                raise Exception()

            for match in child._partial_matches:
                aggregated_events.add_item(match)

        return aggregated_events

    def modified_eval(self, events: Stream, matches: Stream, is_async=False, file_path=None, time_limit: int = None):
        input_stream = None

        if self.all_children_done():
            if self._has_leafs:
                input_stream = events
            else:
                input_stream = self.get_partial_matches_from_children()

            input_stream.close()

            if self._has_leafs:
                self.eval_util_with_leafs(input_stream, matches, is_async, file_path, time_limit)
            else:
                self.eval_util_no_leafs(input_stream, matches, is_async, file_path, time_limit)
            self.light_is_done()
        else:
            time.sleep(0.5)

    def run_eval(self, event_stream, pattern_matches, is_async, file_path, time_limit):  # thread running
        #print('Running thread with id', threading.get_ident())

        if type(self.root) is not ParallelUnaryNode:
            # tree not built properly
            raise Exception()

        while not self._is_done:
            self.modified_eval(event_stream, pattern_matches, is_async, file_path, time_limit)

        #print('Finished running thread with id', threading.get_ident())
        return

    def eval(self, event_stream, pattern_matches, is_async=False, file_path=None, time_limit: int = None):
        print('Running MAIN thread with id', threading.get_ident())

        self.thread = threading.Thread(target=self.run_eval, args=(event_stream, pattern_matches, is_async,

                                                              file_path, time_limit,))
        self.thread.start()

    def all_children_done(self):
        if self.all_children_done_flag == False:
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
            self.all_children_done_flag = True
            return True
        else:
            return True

    def eval_util_with_leafs(self, events: Stream, matches: Stream, is_async=False, file_path=None, time_limit: int = None):
        """
        Activates the tree evaluation mechanism on the input event stream and reports all found patter matches to the
        given output stream.
        """
        self._evaluation_mechanism.__register_event_listeners() #returns a list of event_type with all the leaves that receives this event_type

        start_time = time.time()
        for event in events: #go over the event_stream
            if time_limit is not None:
                if time.time() - start_time > time_limit:
                    matches.close()
                    return
            if event.type not in self._evaluation_mechanism.__event_types_listeners.keys():#if no leaves receives this type of event
                continue
            self._evaluation_mechanism.__remove_expired_freezers(event)
            for leaf in self._evaluation_mechanism.__event_types_listeners[event.type]:
                if self._evaluation_mechanism.__should_ignore_events_on_leaf(leaf):#if there is a freezer in place
                    continue
                self._evaluation_mechanism.__try_register_freezer(event, leaf)#check if the current event is a freezer
                leaf.handle_event(event) #try to get the event through the tree => the real work
            for match in self._evaluation_mechanism.__tree.get_matches():#for all the matches in the root
                matches.add_item(PatternMatch(match))
                self._evaluation_mechanism.__remove_matched_freezers(match)
                if is_async:
                        f = open(file_path, "a", encoding='utf-8')
                        for itr in match:
                            f.write("%s \n" % str(itr.payload))
                        f.write("\n")
                        f.close()

        # Now that we finished the input stream, if there were some pending matches somewhere in the tree, we will
        # collect them now
        for match in self.__tree.get_last_matches():
            matches.add_item(PatternMatch(match))
        matches.close()


    def eval_util_no_leafs(self, partail_matches: Stream, matches: Stream, is_async=False, file_path=None, time_limit: int = None):

            # TODO:
        # start_time = time.time()
        #
        # for event in events:  # go over the event_stream
        #     if time_limit is not None:
        #         if time.time() - start_time > time_limit:
        #             matches.close()
        #             return

        unary_children = self.get_my_unary_children()

        for child in unary_children:
            partail_matches = child.get_partial_matches()
            for pm in partail_matches:
                child.handle_event(pm)

        if self.is_main_root:
            for match in self._evaluation_mechanism.__tree.get_matches():  # for all the matches in the root
                matches.add_item(PatternMatch(match))
                self._evaluation_mechanism.__remove_matched_freezers(match)
                if is_async:
                    f = open(file_path, "a", encoding='utf-8')
                    for itr in match:
                        f.write("%s \n" % str(itr.payload))
                    f.write("\n")
                    f.close()

        for match in self.__tree.get_last_matches():
            matches.add_item(PatternMatch(match))
        matches.close()