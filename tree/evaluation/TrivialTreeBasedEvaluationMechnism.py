import heapq
from datetime import datetime
from base.Event import Event
from stream.Stream import OutputStream
from tree.Tree import Tree
from tree.evaluation.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism


class TrivialTreeBasedEvaluationMechanism(TreeBasedEvaluationMechanism):
    """
    Represent the trivial tree based evaluation mechanism.
    Whenever a new tree is given, replaces the old tree with the new one.
    """

    def _tree_update(self, new_tree: Tree, tree_update_time: datetime):
        """
        Directly replaces the old tree with the new tree.
        """
        old_events = self.__get_all_old_events()
        old_tree = self._tree
        self._tree = new_tree

        self._event_types_listeners = self._register_event_listeners(new_tree)
        self.__play_old_events_on_tree(old_events)

        # To avoid duplicate matches, flushes the matches from the new tree that have already been written.
        for _ in self._tree.get_matches():
            pass

    def __get_all_old_events(self):
        """
        get list of all old events that already played on the old tree
        """
        old_pattern_matches_events = []
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

    def _play_new_event_on_tree(self, event: Event, matches: OutputStream):
        self._play_new_event(event, self._event_types_listeners)

