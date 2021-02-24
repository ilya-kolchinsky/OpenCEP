import heapq
from datetime import datetime

from base.Event import Event
from stream.Stream import OutputStream
from tree.Tree import Tree
from tree.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism


class TrivialEvaluation(TreeBasedEvaluationMechanism):

    def _tree_update(self, new_tree: Tree, tree_update_time: datetime):
        old_events = self.__get_all_old_events()
        self._tree = new_tree
        self._event_types_listeners = self._register_event_listeners(new_tree)
        self.__play_old_events_on_tree(old_events)

        # If there are new matches that have already been written
        # to a file then all we want is to avoid duplications.
        # the loop over get_matches() takes out the duplicate matches.
        for _ in self._tree.get_matches():
            pass

    def __get_all_old_events(self):
        old_pattern_matches_events = []  # todo check the name
        leaf_types = set()
        leaves = self._tree.get_leaves()
        for leaf in leaves:
            leaf_type = leaf.get_event_type()
            if leaf_type not in leaf_types:
                leaf_types.add(leaf_type)
                partial_matches = leaf.get_storage_unit()
                old_pattern_matches_events.append([pm.events[0] for pm in partial_matches])

        # using heap for fast sort of sorted lists
        old_events = list(heapq.merge(*old_pattern_matches_events, key=lambda event: event.timestamp))
        return old_events

    def __play_old_events_on_tree(self, events):
        """
        These events dont need to ask about freeze
        """
        for event in events:
            for leaf in self._event_types_listeners[event.type]:
                leaf.handle_event(event)

    def _is_need_new_statistics(self):
        """
        The Definition of trivial evaluation
        """
        return True

    def _play_new_event_on_tree(self, event: Event, matches: OutputStream):
        self._play_new_event(event, self._event_types_listeners)

    def _get_last_matches(self, matches):
        super()._collect_pending_matches(self._tree, matches)
