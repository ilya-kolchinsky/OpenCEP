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
from tree.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism, PatternMatch


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

    def _tree_update(self, new_tree: Tree):
        self.__tree_update_time = datetime.now()
        self.__new_tree = new_tree
        self.__new_event_types_listeners = self._register_event_listeners(self.__new_tree)
        self.__is_simultaneous_state = True

    def _is_need_new_statistics(self):
        return not self.__is_simultaneous_state

    def _play_new_event_on_tree(self, event: Event, matches: OutputStream):
        self._play_new_event(event, self._event_types_listeners)
        if self.__is_simultaneous_state:
            self._play_new_event(event, self.__new_event_types_listeners)

            # After this round we ask if we are in a simultaneous state.
            # If the time window is over then we want to return to a state that is not simultaneous,
            # i.e. a single tree
            if datetime.now() - self.__tree_update_time > self._pattern.window:
                self._tree, self.__new_tree = self.__new_tree, None
                self._event_types_listeners, self.__new_event_types_listeners = self.__new_event_types_listeners, None
                self.__is_simultaneous_state = False

    def _get_matches(self, matches: OutputStream):
        if self.__is_simultaneous_state:
            for match in self._tree.get_matches():
                if not self.__is_all_new_event(match):
                    matches.add_item(match)
                    self._remove_matched_freezers(match.events)
            for match in self.__new_tree.get_matches():
                matches.add_item(match)
                self._remove_matched_freezers(match.events)
        else:
            super()._get_matches(matches)

    def __is_all_new_event(self, match: PatternMatch):
        """
        Checks whether all the events in the match are new
        """
        for event in match.events:
            if event.arrival_time < self.__tree_update_time:
                return False
        return True

    def _get_last_matches(self, matches):
        super()._collect_pending_matches(self._tree, matches)
        if self.__new_tree is not None:
            super()._collect_pending_matches(self.__new_tree, matches)
