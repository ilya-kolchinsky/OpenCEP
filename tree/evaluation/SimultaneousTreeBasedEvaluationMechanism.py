from datetime import timedelta, datetime
from typing import Dict
from base.Event import Event
from base.Pattern import Pattern
from adaptive.optimizer.Optimizer import Optimizer
from plan.TreePlan import TreePlan
from adaptive.statistics.StatisticsCollector import StatisticsCollector
from stream.Stream import OutputStream
from tree.PatternMatchStorage import TreeStorageParameters
from tree.Tree import Tree
from tree.evaluation.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism


class SimultaneousTreeBasedEvaluationMechanism(TreeBasedEvaluationMechanism):
    """
    Whenever a new tree is given,
    Runs the old tree and the new tree in parallel as long as the events affect the matches from the trees differently,
    after that, replaces the old tree with the new tree.
    """
    def __init__(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                 storage_params: TreeStorageParameters,
                 statistics_collector: StatisticsCollector = None,
                 optimizer: Optimizer = None,
                 statistics_update_time_window: timedelta = None):
        super().__init__(pattern_to_tree_plan_map, storage_params,
                         statistics_collector,
                         optimizer,
                         statistics_update_time_window)
        self.__new_tree = None
        self.__new_event_types_listeners = None
        self.__is_simultaneous_state = False
        self.__tree_update_time = None
        self.__last_matches_from_old_tree = None

    def _tree_update(self, new_tree: Tree, tree_update_time: datetime):
        """
        Registers a new tree and moves from single tree state to simultaneous state
        where the events are played in a parallel fashion on both trees.
        """
        self.__tree_update_time = tree_update_time
        self.__new_tree = new_tree
        self.__new_event_types_listeners = self._register_event_listeners(self.__new_tree)
        self.__is_simultaneous_state = True

    def _should_try_reoptimize(self, last_statistics_refresh_time: timedelta, last_event: Event):
        """
        If the simultaneous state is activated, there is no need for new statistics.
        This function avoids a situation where there are more than two trees in parallel.
        """
        if self.__is_simultaneous_state:
            return False
        return super()._should_try_reoptimize(last_statistics_refresh_time, last_event)

    def _play_new_event_on_tree(self, event: Event, matches: OutputStream):
        if self.__is_simultaneous_state:
            self.__play_new_event_on_new_tree(event, self.__new_event_types_listeners)

        self._play_new_event(event, self._event_types_listeners)
        if self.__is_simultaneous_state:
            # After this round we ask if we are in a simultaneous state.
            # If the pattern window is over then we want to return to single tree state.
            if event.timestamp - self.__tree_update_time > self._pattern.window:
                # Passes pending matches from the old tree to the new tree if the root is a NegationNode
                self.__last_matches_from_old_tree = self._tree.get_last_matches()

                # Tree replacement and a return to single tree state
                self._tree, self.__new_tree = self.__new_tree, None
                self._event_types_listeners, self.__new_event_types_listeners = self.__new_event_types_listeners, None
                self.__is_simultaneous_state = False

    def _get_matches(self, matches: OutputStream):
        super()._get_matches(matches)
        if self.__is_simultaneous_state:
            # Flush the matches from the new tree while in simultaneous state
            # These matches were necessarily obtained from the old tree
            for _ in self.__new_tree.get_matches():
                pass
        elif self.__last_matches_from_old_tree is not None:
            # in non-simultaneous state, it is possible that we just deleted the old tree but its last matches
            # went unreported
            for match in self.__last_matches_from_old_tree:
                matches.add_item(match)
            self.__last_matches_from_old_tree = None

    def __play_new_event_on_new_tree(self, event: Event, event_types_listeners):
        """
        Lets the new tree handle the event
        """
        for leaf in event_types_listeners[event.type]:
            if self._should_ignore_events_on_leaf(leaf, event_types_listeners):
                continue
            leaf.handle_event(event)
