from typing import List, Set
from functools import reduce
from misc.Utils import calculate_joint_probability

from base.Event import Event, AggregatedEvent
from condition.CompositeCondition import CompositeCondition
from base.PatternMatch import PatternMatch
from misc.Utils import powerset_generator
from tree.nodes.Node import Node, PatternParameters
from tree.nodes.UnaryNode import UnaryNode


class KleeneClosureNode(UnaryNode):
    """
    An internal node representing a Kleene closure operator.
    It generates and propagates sets of partial matches provided by its sole child.
    """
    def __init__(self, pattern_params: PatternParameters, min_size, max_size,
                 parents: List[Node] = None, pattern_ids: int or Set[int] = None):
        super().__init__(pattern_params, parents, pattern_ids)
        self.__min_size = min_size
        self.__max_size = max_size

    def handle_new_partial_match(self, partial_match_source: Node):
        """
        Reacts upon a notification of a new partial match available at the child by generating, validating, and
        propagating all sets of partial matches containing this new partial match.
        Note: this method strictly assumes that the last partial match in the child storage is the one to cause the
        method call (could not function properly in a parallelized implementation of the evaluation tree).
        """
        if self._child is None:
            raise Exception()  # should never happen

        new_partial_match = self._child.get_last_unhandled_partial_match_by_parent(self)
        self._child.clean_expired_partial_matches(new_partial_match.last_timestamp)

        # create partial match sets containing the new partial match that triggered this method
        child_matches_powerset = self.__create_child_matches_powerset()

        for partial_match_set in child_matches_powerset:
            # create and propagate the new match
            all_primitive_events = reduce(lambda x, y: x + y, [pm.events for pm in partial_match_set])
            probability = None if self._confidence is None else \
                reduce(calculate_joint_probability, (pm.probability for pm in partial_match_set), None)
            aggregated_event = AggregatedEvent(all_primitive_events, probability)
            self._validate_and_propagate_partial_match([aggregated_event], probability)

    def _validate_new_match(self, events_for_new_match: List[Event]):
        """
        Validates the condition stored in this node on the given set of events.
        """
        if len(events_for_new_match) != 1 or not isinstance(events_for_new_match[0], AggregatedEvent):
            raise Exception("Unexpected candidate event list for Kleene closure operator")
        if not Node._validate_new_match(self, events_for_new_match):
            return False
        return self._condition.eval([e.payload for e in events_for_new_match[0].primitive_events])

    def __create_child_matches_powerset(self):
        """
        This method is a generator returning all subsets of currently available partial matches of this node child.
        As this method is always invoked following a notification regarding a new partial match received from the child,
        only the subsets containing this new partial match (which is assumed to be the last partial match in the child
        list) are generated.
        The subsets are enforced to satisfy the minimal and maximal size constraints.
        The maximal size constraint is enforced recursively to save as many computations as possible.
        The minimal size constraint on the other hand is enforced via post-processing filtering due to negligible
        overhead.
        """
        child_partial_matches = self._child.get_partial_matches()
        if len(child_partial_matches) == 0:
            return []
        last_partial_match = child_partial_matches[-1]
        # create subsets for all but the last element
        actual_max_size = self.__max_size if self.__max_size is not None else len(child_partial_matches)
        generated_powerset = powerset_generator(child_partial_matches[:-1], actual_max_size - 1)
        # add the last item to all previously created subsets
        result_powerset = [item + [last_partial_match] for item in generated_powerset]
        # enforce minimal size limit
        result_powerset = [item for item in result_powerset if self.__min_size <= len(item)]
        return result_powerset

    def apply_condition(self, condition: CompositeCondition):
        """
        The default implementation is overridden to extract KC conditions from the given composite condition.
        """
        self._propagate_condition(condition)
        names = {event_def.name for event_def in self.get_event_definitions()}
        self._condition = condition.get_condition_of(names, get_kleene_closure_conditions=True,
                                                     consume_returned_conditions=True)

    def get_structure_summary(self):
        return "KC", self._child.get_structure_summary()

    def is_equivalent(self, other):
        """
        In addition to the checks performed by the base class, compares the min_size and max_size fields.
        """
        if not super().is_equivalent(other):
            return False
        return self.__min_size == other.__min_size and self.__max_size == other.__max_size
