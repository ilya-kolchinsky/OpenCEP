from typing import Dict

from base.DataFormatter import DataFormatter
from base.Event import Event
from plan.TreePlan import TreePlan
from stream.Stream import InputStream, OutputStream
from misc.Utils import *
from tree.nodes.LeafNode import LeafNode
from tree.PatternMatchStorage import TreeStorageParameters
from evaluation.EvaluationMechanism import EvaluationMechanism
from misc.ConsumptionPolicy import *
from plan.multi.MultiPatternEvaluationParameters import MultiPatternEvaluationParameters
from tree.MultiPatternTree import MultiPatternTree

from tree.Tree import Tree


class TreeBasedEvaluationMechanism(EvaluationMechanism):
    """
    An implementation of the tree-based evaluation mechanism.
    """
    def __init__(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                 storage_params: TreeStorageParameters,
                 multi_pattern_eval_params: MultiPatternEvaluationParameters = MultiPatternEvaluationParameters()):

        is_multi_pattern_mode = len(pattern_to_tree_plan_map) > 1
        if is_multi_pattern_mode:
            self.__tree = MultiPatternTree(pattern_to_tree_plan_map, storage_params, multi_pattern_eval_params)
        else:
            self.__tree = Tree(list(pattern_to_tree_plan_map.values())[0],
                               list(pattern_to_tree_plan_map)[0], storage_params)

        self.__event_types_listeners = {}

        # The remainder of the initialization process is only relevant for the freeze map feature. This feature can
        # only be enabled in single-pattern mode.
        self.__pattern = list(pattern_to_tree_plan_map)[0] if not is_multi_pattern_mode else None
        self.__freeze_map = {}
        self.__active_freezers = []
        if not is_multi_pattern_mode and self.__pattern.consumption_policy is not None and \
                self.__pattern.consumption_policy.freeze_names is not None:
            self.__init_freeze_map()

    def eval(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """
        Activates the tree evaluation mechanism on the input event stream and reports all found pattern matches to the
        given output stream.
        """
        self.__register_event_listeners()
        for raw_event in events:
            event = Event(raw_event, data_formatter)
            if event.type not in self.__event_types_listeners.keys():
                continue
            self.__remove_expired_freezers(event)
            for leaf in self.__event_types_listeners[event.type]:
                if self.__should_ignore_events_on_leaf(leaf):
                    continue
                self.__try_register_freezer(event, leaf)
                leaf.handle_event(event)
            for match in self.__tree.get_matches():
                matches.add_item(match)
                self.__remove_matched_freezers(match.events)

        # Now that we finished the input stream, if there were some pending matches somewhere in the tree, we will
        # collect them now
        for match in self.__tree.get_last_matches():
            matches.add_item(match)
        matches.close()

    def eval_parallel(self, events: InputStream, matches: Stream, data_formatter: DataFormatter, time1, time2):
        self.__register_event_listeners()
        countM = 0
        countE = 0
        for raw_event in events:
            countE+=1
            event = Event(raw_event, data_formatter)
            if event.type not in self.__event_types_listeners.keys():
                continue
            self.__remove_expired_freezers(event)
            for leaf in self.__event_types_listeners[event.type]:
                if self.__should_ignore_events_on_leaf(leaf):
                    continue
                self.__try_register_freezer(event, leaf)
                leaf.handle_event(event)
            ######

            if event.timestamp <= time1: #begin
                for match in self.__tree.get_matches():
                    match_event = match.events
                    flag = True
                    for event in match_event:
                        if event.timestamp > time1: #not need to check if duplicated
                            flag = False
                            break
                    matches.add_item([match, flag])
                    self.__remove_matched_freezers(match.events)
                    countM+=1
            elif event.timestamp >= time2:# end
                for match in self.__tree.get_matches():
                    match_event = match.events
                    flag = True
                    for event in match_event:
                        if event.timestamp < time2:  # not need to check if duplicated
                            flag = False
                            break
                    matches.add_item([match, flag])
                    self.__remove_matched_freezers(match.events)
                    countM+=1
            else: #middle
                for match in self.__tree.get_matches():
                    matches.add_item([match, False])
                    self.__remove_matched_freezers(match.events)

        # Now that we finished the input stream, if there were some pending matches somewhere in the tree, we will
        # collect them now
        for match in self.__tree.get_last_matches():
            matches.add_item([match, True])
            countM+=1

        #print("m ", countM)

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

    def __remove_matched_freezers(self, match_events: List[Event]):
        """
        Removes the freezers that have been matched.
        """
        if len(self.__freeze_map) == 0:
            # freeze option disabled
            return False
        self.__active_freezers = [freezer for freezer in self.__active_freezers if freezer not in match_events]

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

    def __repr__(self):
        return self.get_structure_summary()
