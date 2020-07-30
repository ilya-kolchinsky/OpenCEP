from enum import Enum
from misc.IOUtils import Stream
from abc import ABC
from datetime import timedelta, datetime
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, QItem
from base.Formula import TrueFormula, Formula
from evaluation.PartialMatch import PartialMatch
from misc.IOUtils import Stream
from typing import List, Tuple
from base.Event import Event
from datetime import datetime
from misc.Utils import merge, merge_according_to, is_sorted, find_partial_match_by_timestamp
from base.PatternMatch import PatternMatch
from evaluation.EvaluationMechanism import EvaluationMechanism
from queue import Queue
from evaluation.AdaptiveTreeBasedEvaluationMechanism import Tree, AdaptiveTreeBasedEvaluationMechanism
import time


class TreeReplacementAlgorithmTypes(Enum):
    IMMEDIATE_REPLACE_TREE = 0,
    SIMULTANEOUSLY_RUN_TWO_TREES = 1,
    COMMON_PARTS_SHARE = 2


"""
Compare 2 matches according to their events
"""
def compare_matches(match1: List[Event], match2: List[Event]):
    if len(match1) != len(match2):
        return False
    for event1 in match1:
        if event1 not in match2:
            return False
    return True


"""
Creating an adaptive evaluation mechanism according to the tree replacement algorithm type
"""
def create_adaptive_evaluation_mechanism_by_type(handler_type: TreeReplacementAlgorithmTypes, pattern: Pattern,
                                                 initial_tree: Tree):
    if handler_type == TreeReplacementAlgorithmTypes.IMMEDIATE_REPLACE_TREE:
        return ImmediateReplaceTree(pattern, initial_tree)
    if handler_type == TreeReplacementAlgorithmTypes.SIMULTANEOUSLY_RUN_TWO_TREES:
        return
    if handler_type == TreeReplacementAlgorithmTypes.COMMON_PARTS_SHARE:
        raise NotImplementedError()


class ImmediateReplaceTree(AdaptiveTreeBasedEvaluationMechanism):
    def __init__(self, pattern: Pattern, initial_tree: Tree):
        self.active_events = Stream()  # A stream of the current events
        super().__init__(pattern, initial_tree)

    """
    Removing out of date events from the stream active_events
    """
    def __remove_expired_events(self, curr_time: datetime):
        while self.active_events.count != 0:
            last_event = self.active_events.first()
            if curr_time - last_event.timestamp > self.pattern.window:
                self.active_events.get_item()
            else:
                return

    """
    Inserting the events from the active_events stream into the new tree to be replaced 
    and handling the partial matches in it
    """
    def __update_current_events_in_new_tree(self, new_tree):
        self.__tree = new_tree
        self.event_types_listeners = super().find_event_types_listeners(self.__tree)
        for event in self.active_events:
            if event.event_type in self.event_types_listeners.keys():
                for leaf in self.event_types_listeners[event.event_type]:
                    leaf.handle_event(event)

    def eval(self, event: Event, new_tree: Tree, matches: Stream, statistics_collector: StatisticsCollector):
        self.__remove_expired_events(event.timestamp)
        if new_tree is not None:
            self.__update_current_events_in_new_tree(new_tree)
        # Send events to listening leaves.
        if event.event_type in self.event_types_listeners.keys():
            statistics_collector.insert_event(event)
            for leaf in self.event_types_listeners[event.event_type]:
                partial_matches = self.find_partial_matches(event, self.event_types_listeners)
                statistics_collector.update_selectivitys_matrix(event, partial_matches)
                leaf.handle_event(event)
                for match in self.__tree.get_matches():
                    matches.add_item(PatternMatch(match))
        self.active_events.add_item(event)  # TODO: Make sure that's how you insert into a stream

    def find_partial_matches(self, event: Event, event_types_listeners):
        count = 0
        args = self.pattern.structure.args
        match_count = {}
        for arg in args:
            match_count[arg.event_type] = []

        event_name = event_types_listeners[event.event_type][0]._LeafNode__event_name

        for event_type in event_types_listeners.keys():
            if event.event_type == event_type:
                continue
            else:
                for events_list in event_types_listeners[event_type]:
                    if len(events_list._partial_matches) == 0:
                        continue
                    else:
                        for events_to_check in events_list._partial_matches:
                            for event_to_check in events_to_check.events:
                                count += 1
                                event_to_check_name = event_types_listeners[event_to_check.event_type][0].\
                                    _LeafNode__event_name
                                formula = self.pattern.condition.get_formula_of({event_name, event_to_check_name})
                                if formula is not None:
                                    if formula.eval(
                                            {event_name: event.payload, event_to_check_name: event_to_check.payload}):
                                        match_count[event_type].insert(len(match_count[event_type]),
                                                                       event_to_check.timestamp)
        return match_count


class SimultaneouslyRunTwoTrees(AdaptiveTreeBasedEvaluationMechanism):
    def __init__(self, pattern: Pattern, initial_tree: Tree):
        super().__init__(pattern, initial_tree)
        self.__tree2 = None
        self.event_types_listeners2 = None
        self.tree2_create_time = None

    def eval(self, event: Event, new_tree: Tree, matches: Stream, statistics_collector: StatisticsCollector):
        # TODO: When a non None new_tree arrive, we replace it with self.__tree2
        # Checking if tree 1 should be replaced with tree 2 because the time window expired
        # If replacement is needed then initialize tree1 with tree2 data
        if self.__tree2 is not None:
            if time.time() - self.tree2_create_time > self.pattern.window:
                self.__tree = self.__tree2
                self.event_types_listeners = self.event_types_listeners2
                self.__tree2 = None
                self.event_types_listeners2 = None
                self.tree2_create_time = None
        # Checking if a new plan was received from Optimizer
        if new_tree is not None:
            self.__tree2 = new_tree
            self.event_types_listeners2 = self.find_event_types_listeners(self.__tree2)
            self.tree2_create_time = time.time()
        # Insert and handles the incoming event in tree1
        if event.event_type in self.event_types_listeners.keys():
            for leaf in self.event_types_listeners[event.event_type]:
                leaf.handle_event(event)
                for match in self.__tree.get_matches():
                    matches.add_item(PatternMatch(match))
        if self.__tree2 is not None:
            # Insert and handles the incoming event in tree2
            if event.event_type in self.event_types_listeners2.keys():
                for leaf in self.event_types_listeners2[event.event_type]:
                    leaf.handle_event(event)
                    for match in self.__tree2.get_matches():
                        match_exists = False
                        for tree1_match in self.__tree.get_matches():
                            if compare_matches(match, tree1_match):
                                match_exists = True
                                break
                        if match_exists is False:
                            matches.add_item(PatternMatch(match))



