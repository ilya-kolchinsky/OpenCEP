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


class SimultaneousTreeBasedEvaluationMechanism(TreeBasedEvaluationMechanism):
    """
    Whenever a new tree is given,
    Runs the old tree and the new tree in parallel as long as the events affect the matches from the trees differently,
    after that, replaces the old tree with the new tree.
    """
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
        """
        Registers a new tree and moves from single tree state to simultaneous state
        where the events are played in a parallel fashion on both trees.
        """
        self.__tree_update_time = tree_update_time
        self.__new_tree = new_tree
        self.__new_event_types_listeners = self._register_event_listeners(self.__new_tree)
        self.__is_simultaneous_state = True

    def _is_need_new_statistics(self):
        """
        If the simultaneous state is activated, theres no need for new statistics.
        This function avoids a situation where there are more than two trees in parallel.
        """
        return not self.__is_simultaneous_state

    def _play_new_event_on_tree(self, event: Event, matches: OutputStream):
        if self.__is_simultaneous_state:
            self.__play_new_event_on_new_tree(event, self.__new_event_types_listeners)

        self._play_new_event(event, self._event_types_listeners)
        if self.__is_simultaneous_state:
            # After this round we ask if we are in a simultaneous state.
            # If the pattern window is over then we want to return to single tree state.
            if event.timestamp - self.__tree_update_time > self._pattern.window:
                # Passes pending matches from the old tree to the new tree if the root is a NegationNode
                old_pending_matches = self._tree.get_last_matches()
                if old_pending_matches:
                    self.__new_tree.set_pending_matches(old_pending_matches)

                # Tree replacement and a return to single tree state
                self._tree, self.__new_tree = self.__new_tree, None
                self._event_types_listeners, self.__new_event_types_listeners = self.__new_event_types_listeners, None
                self.__is_simultaneous_state = False

    def _get_matches(self, matches: OutputStream):
        super()._get_matches(matches)
        # Flush the matches from the new tree while in simultaneous state
        # These matches were necessarily obtained from the old tree
        if self.__is_simultaneous_state:
            for _ in self.__new_tree.get_matches():
                pass

    def __play_new_event_on_new_tree(self, event: Event, event_types_listeners):
        """
        Lets the new tree handle the event
        """
        for leaf in event_types_listeners[event.type]:
            if self._should_ignore_events_on_leaf(leaf, event_types_listeners):
                continue
            leaf.handle_event(event)
