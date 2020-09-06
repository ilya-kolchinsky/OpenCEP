from base import Pattern
from evaluation.PartialMatchStorage import TreeStorageParameters
from evaluation.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism, UnaryNode, Node
from parallerization.ParallelExecutionFramework import ParallelExecutionFramework
from datetime import timedelta
from typing import Tuple, Dict, List
from base.PatternStructure import QItem
from misc.IOUtils import Stream

import time
import threading


class ParallelUnaryNode(UnaryNode):
    def __init__(self, is_done: bool, sliding_window: timedelta, parent: Node = None,
                 event_defs: List[Tuple[int, QItem]] = None, child: Node = None):
        super().__init__(sliding_window, parent, event_defs, child)
        self._is_done = child

    def get_done(self):
        return self._is_done


class UnaryParallelTree(ParallelExecutionFramework):

    def __init__(self, tree_based_eval: TreeBasedEvaluationMechanism, has_leafs: bool):
        self._tree_based = tree_based_eval
        self._has_leafs = has_leafs

    def stop(self):
        raise NotImplementedError()

    def get_data(self):
        raise NotImplementedError()

    def get_final_results(self, pattern_matches):
        raise NotImplementedError()

    def wait_till_finish(self):
        root = self._tree_based.get_tree().get_root()
        if type(root) is not ParallelUnaryNode:
            # tree not built properly
            raise Exception()
        while not root.get_done():
            time.sleep(0.5)

    def get_children(self):
        raise NotImplementedError()

    def all_children_done(self):
        children = self.get_children()
        if children is None:
            return True
        for child in children:
            child_done = child.get_done.try_to_lock()
            if not child_done:
                child.get_done.unlock()
                return False
            else:
                child.done.unlock()
        return True

    def get_partial_matches_from_children(self):
        raise NotImplementedError()

    def modified_eval(self, events: Stream, matches: Stream, is_async=False, file_path=None, time_limit: int = None):
        if self.all_children_done():
            aggregated_events = self.get_partial_matches_from_children()
            eval_util(aggregated_events)  # original eval
            done = True
        else:
            time.sleep(0.5)

    def run_eval(self, tree_based, event_stream, pattern_matches, is_async, file_path, time_limit):  # thread running
        root = self._tree_based.get_tree().get_root()
        if type(root) is not ParallelUnaryNode:
            # tree not built properly
            raise Exception()
        while not root.get_done():
            self.modified_eval(tree_based, event_stream, pattern_matches, is_async, file_path, time_limit)

    def eval(self, event_stream, pattern_matches, is_async=False, file_path=None, time_limit: int = None):
        thread = threading.Thread(target=self.run_eval, args=(self._tree_based, event_stream, pattern_matches, is_async,
                                                              file_path, time_limit,))
        thread.start()





    """
    An implementation of the tree-based evaluation mechanism.
    """

    def eval_util(self, events: Stream, matches: Stream, is_async=False, file_path=None, time_limit: int = None):
        """
        Activates the tree evaluation mechanism on the input event stream and reports all found patter matches to the
        given output stream.
        """
        self.__register_event_listeners()
        start_time = time.time()
        for event in events:
            if time_limit is not None:
                if time.time() - start_time > time_limit:
                    matches.close()
                    return
            if event.type not in self.__event_types_listeners.keys():
                continue
            self.__remove_expired_freezers(event)
            for leaf in self.__event_types_listeners[event.type]:
                if self.__should_ignore_events_on_leaf(leaf):
                    continue
                self.__try_register_freezer(event, leaf)
                leaf.handle_event(event)
            for match in self.__tree.get_matches():
                matches.add_item(PatternMatch(match))
                self.__remove_matched_freezers(match)
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

    def __register_event_listeners(self):
        """
        Register leaf listeners for event types.
        """
        self.__event_types_listeners = {}
        for leaf in self.__tree.get_leaves():
            event_type = leaf.get_event_type()
            if event_type in self.__event_types_listeners.keys():
                self.__event_types_listeners[event_type].append(leaf)
            else:
                self.__event_types_listeners[event_type] = [leaf]

    def __init_freeze_map(self):
        """
        For each event type specified by the user to be a 'freezer', that is, an event type whose appearance blocks
        initialization of new sequences until it is either matched or expires, this method calculates the list of
        leaves to be disabled.
        """
        sequences = self.__pattern.extract_flat_sequences()
        for freezer_event_name in self.__pattern.consumption_policy.freeze_names:
            current_event_name_set = set()
            for sequence in sequences:
                if freezer_event_name not in sequence:
                    continue
                for name in sequence:
                    current_event_name_set.add(name)
                    if name == freezer_event_name:
                        break
            if len(current_event_name_set) > 0:
                self.__freeze_map[freezer_event_name] = current_event_name_set

    def __should_ignore_events_on_leaf(self, leaf: LeafNode):
        """
        If the 'freeze' consumption policy is enabled, checks whether the given event should be dropped based on it.
        """
        if len(self.__freeze_map) == 0:
            # freeze option disabled
            return False
        for freezer in self.__active_freezers:
            for freezer_leaf in self.__event_types_listeners[freezer.type]:
                if freezer_leaf.get_event_name() not in self.__freeze_map:
                    continue
                if leaf.get_event_name() in self.__freeze_map[freezer_leaf.get_event_name()]:
                    return True
        return False

    def __try_register_freezer(self, event: Event, leaf: LeafNode):
        """
        Check whether the current event is a freezer event, and, if positive, register it.
        """
        if leaf.get_event_name() in self.__freeze_map.keys():
            self.__active_freezers.append(event)

    def __remove_matched_freezers(self, match: List[Event]):
        """
        Removes the freezers that have been matched.
        """
        if len(self.__freeze_map) == 0:
            # freeze option disabled
            return False
        self.__active_freezers = [freezer for freezer in self.__active_freezers if freezer not in match]

    def __remove_expired_freezers(self, event: Event):
        """
        Removes the freezers that have been expired.
        """
        if len(self.__freeze_map) == 0:
            # freeze option disabled
            return False
        self.__active_freezers = [freezer for freezer in self.__active_freezers
                                  if event.timestamp - freezer.timestamp <= self.__pattern.window]

    def get_structure_summary(self):
        return self.__tree.get_structure_summary()
