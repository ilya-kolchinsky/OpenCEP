from datetime import timedelta, datetime
from typing import Dict
from base.Event import Event
from base.Pattern import Pattern
from optimizer.Optimizer import Optimizer
from plan.TreePlan import TreePlan
from plan.multi.MultiPatternEvaluationParameters import MultiPatternEvaluationParameters
from statistics_collector.StatisticsCollector import StatisticsCollector
from stream.Stream import OutputStream
from tree.PatternMatchStorage import TreeStorageParameters
from tree.Tree import Tree
from tree.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism


class SimultaneousEvaluation(TreeBasedEvaluationMechanism):

    def __init__(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                 storage_params: TreeStorageParameters,
                 statistics_collector: StatisticsCollector,
                 optimizer: Optimizer,
                 statistics_update_time_window: timedelta,
                 multi_pattern_eval_params: MultiPatternEvaluationParameters = MultiPatternEvaluationParameters()):
        super().__init__(pattern_to_tree_plan_map, storage_params,
                         statistics_collector,
                         optimizer,
                         statistics_update_time_window,
                         multi_pattern_eval_params)
        self.__new_tree = None
        self.__new_event_types_listeners = None
        self.__is_simultaneous_state = False
        self.__tree_update_time = None

    def _tree_update(self, new_tree: Tree, tree_update_time: datetime):
        self.__tree_update_time = tree_update_time
        self.__new_tree = new_tree
        self.__new_event_types_listeners = self._register_event_listeners(self.__new_tree)
        self.__is_simultaneous_state = True

    def _is_need_new_statistics(self):
        return not self.__is_simultaneous_state

    def _play_new_event_on_tree(self, event: Event, matches: OutputStream):
        if self.__is_simultaneous_state:
            self._play_new_event_on_new(event, self.__new_event_types_listeners)

        self._play_new_event(event, self._event_types_listeners)
        if self.__is_simultaneous_state:
            # After this round we ask if we are in a simultaneous state.
            # If the time window is over then we want to return to a state that is not simultaneous,
            # i.e. a single tree
            if event.timestamp - self.__tree_update_time > self._pattern.window:
                # Passes pending matches from the old tree to the new tree if the root is a NegationNode
                old_pending_matches = self._tree.get_last_matches()
                if old_pending_matches:
                    self.__new_tree.set_pending_matches(old_pending_matches)

                self._tree, self.__new_tree = self.__new_tree, None
                self._event_types_listeners, self.__new_event_types_listeners = self.__new_event_types_listeners, None
                self.__is_simultaneous_state = False

    def _get_matches(self, matches: OutputStream):
        if self.__is_simultaneous_state:
            # TODO: comment here
            for match in self._tree.get_matches():
                matches.add_item(match)
                self._remove_matched_freezers(match.events)
            # Flush the matches from the new tree while in simultaneous state
            for _ in self.__new_tree.get_matches():
                pass
        else:
            super()._get_matches(matches)

    def _get_last_pending_matches(self, matches):
        """
        pending matches in the new tree are contained in the old tree
        therefore it is enough to collect the pending matches only from the old tree
        """
        super()._collect_pending_matches(self._tree, matches)

    def _play_new_event_on_new(self, event: Event, event_types_listeners):
        """
        Lets the tree handle the event
        """
        for leaf in event_types_listeners[event.type]:
            if self._should_ignore_events_on_leaf(leaf, event_types_listeners):
                continue
            leaf.handle_event(event)
