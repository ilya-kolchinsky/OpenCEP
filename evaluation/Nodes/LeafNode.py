from Node import Node
from datetime import timedelta, datetime
from base.Formula import TrueFormula, Formula
from evaluation.PartialMatch import PartialMatch
from base.PatternStructure import SeqOperator, QItem
from base.Event import Event


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

    def add_partial_match(self, pm: PartialMatch):
        self._partial_matches.append(pm)

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

        self.add_partial_match(PartialMatch([event]))
        if self._parent is not None:
            self._parent.handle_new_partial_match(self)
