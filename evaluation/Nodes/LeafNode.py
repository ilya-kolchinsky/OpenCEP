from evaluation.Nodes.Node import Node
from datetime import timedelta, datetime
from base.Formula import TrueFormula, Formula
from evaluation.PartialMatch import PartialMatch
from base.PatternStructure import SeqOperator, QItem

# from base.Event import Event #TODO
from evaluation.temp_simple_modules import Event
from evaluation.Storage import SortedStorage, UnsortedStorage


class LeafNode(Node):
    """
    A leaf node is responsible for a single event type of the pattern.
    """

    def __init__(
        self,
        sliding_window: timedelta,
        leaf_index: int,
        leaf_qitem: QItem,
        parent: Node,
    ):
        super().__init__(sliding_window, parent)
        self.__leaf_index = leaf_index
        self.__event_name = leaf_qitem.name
        self.__event_type = leaf_qitem.event_type

    def json_repr(self):
        return {
            "Node": "LeafNode",
            "event name": self.__event_name,
            "event type": self.__event_type,
            "leaf index": self.__leaf_index,
            "condition": repr(self._condition),
            "pms": repr(self._partial_matches),
        }

    def add_partial_match(self, pm: PartialMatch):
        self._partial_matches.append(pm)
        # self._partial_matches.add(pm)

        if self._parent is not None:
            self._unhandled_partial_matches.put(pm)

    def create_storage_unit(
        self, sorting_key: callable, is_sorted_by_first_timestamp=False
    ):
        # in case of a leaf node even if sorting_key is None we create SortedStorage and it would be sorted based on timestamps
        if sorting_key is None:  # this means no condition and no sequence requested
            self._partial_matches = SortedStorage([], lambda x: x.first_timestamp, True)

        else:  # either by condition or by sequence
            self._partial_matches = SortedStorage(
                [], sorting_key, is_sorted_by_first_timestamp
            )

    def get_leaves(self):
        return [self]

    def apply_formula(self, formula: Formula):  # formula: is the original formula
        condition = formula.get_formula_of(self.__event_name)
        self._condition = condition if condition else TrueFormula()
        # formula doesn't need to be simplified

    def get_event_definitions(self):
        return [(self.__leaf_index, QItem(self.__event_type, self.__event_name))]

    def get_event_type(self):
        """
        Returns the type of events processed by this leaf.
        """
        return self.__event_type

    def handle_event(self, event: Event):
        """
        Inserts the given event to this leaf.
        """
        self.clean_expired_partial_matches(event.timestamp)

        # get event's qitem and make a binding to evaluate formula for the new event.
        binding = {self.__event_name: event.payload}

        if not self._condition.eval(binding):
            return
        # right now it only appends a partial match to storage
        # TODO : When we work on condition we should change to inserting in O(logn)
        print("just BEFORE adding a partial match to the leaf")
        self.add_partial_match(PartialMatch([event]))
        print("just AFTER adding a partial match to the leaf")
        if self._parent is not None:
            self._parent.handle_new_partial_match(self)
