from base.Event import Event
from evaluation.Nodes.Node import Node
from evaluation.Nodes.InternalNode import AndNode
from datetime import timedelta, datetime
from base.Formula import TrueFormula, Formula
from evaluation.PartialMatch import PartialMatch
from base.PatternStructure import SeqOperator, QItem
from evaluation.Storage import SortedStorage, UnsortedStorage


class LeafNode(Node):
    """
    A leaf node is responsible for a single event type of the pattern.
    """

    def __init__(
        self, sliding_window: timedelta, leaf_index: int, leaf_qitem: QItem, parent: Node,
    ):
        super().__init__(sliding_window, parent)
        self.__leaf_index = leaf_index
        self.__event_name = leaf_qitem.name
        self.__event_type = leaf_qitem.event_type

    def get_leaves(self):
        return [self]

    def apply_formula(self, formula: Formula):
        if formula is None:
            return
        condition = formula.get_formula_of(self.__event_name)
        if condition is not None:
            self._condition = condition

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

        self.add_partial_match(PartialMatch([event]))
        if self._parent is not None:
            self._parent.handle_new_partial_match(self)

    def add_partial_match(self, pm: PartialMatch):
        if self._parent is not None:
            self._unhandled_partial_matches.put(pm)
            if type(self._parent) == AndNode:
                self._partial_matches.add(pm)
                return
        self._partial_matches.append(pm)

    def create_storage_unit(self, sorting_key: callable = None, relation_op=None, equation_side=None):
        # in case of a leaf node even if sorting_key is None we create SortedStorage and it would be sorted based on timestamps
        if sorting_key is None:  # this means no condition and no sequence requested
            self._partial_matches = UnsortedStorage([])
            """SortedStorage([], lambda pm: pm.first_timestamp, relation_op, equation_side)"""

        else:  # either by condition or by sequence
            self._partial_matches = SortedStorage([], sorting_key, relation_op, equation_side)

    def json_repr(self):
        return {
            "Node": "LeafNode",
            "event name": self.__event_name,
            "event type": self.__event_type,
            "leaf index": self.__leaf_index,
            "condition": repr(self._condition),
            "pms": repr(self._partial_matches),
        }
