from abc import ABC
from datetime import timedelta
from typing import List, Set

from base.Event import Event
from misc.Utils import calculate_joint_probability
from condition.Condition import Condition, Variable, EquationSides
from condition.BaseRelationCondition import BaseRelationCondition
from base.PatternMatch import PatternMatch
from tree.nodes.InternalNode import InternalNode
from tree.nodes.Node import Node, PrimitiveEventDefinition, PatternParameters


class BinaryNode(InternalNode, ABC):
    """
    An internal node connects two subtrees, i.e., two subpatterns of the evaluated pattern.
    """
    def __init__(self, pattern_params: PatternParameters, parents: List[Node] = None, pattern_ids: int or Set[int] = None,
                 event_defs: List[PrimitiveEventDefinition] = None,
                 left: Node = None, right: Node = None):
        super().__init__(pattern_params, parents, pattern_ids, event_defs)
        self._left_subtree = left
        self._right_subtree = right

    def create_parent_to_info_dict(self):
        if self._left_subtree is not None:
            self._left_subtree.create_parent_to_info_dict()
        if self._right_subtree is not None:
            self._right_subtree.create_parent_to_info_dict()
        super().create_parent_to_info_dict()

    def get_leaves(self):
        """
        Returns all leaves in this tree.
        """
        result = []
        if self._left_subtree is not None:
            result += self._left_subtree.get_leaves()
        if self._right_subtree is not None:
            result += self._right_subtree.get_leaves()
        return result

    def _propagate_condition(self, condition: Condition):
        self._left_subtree.apply_condition(condition)
        self._right_subtree.apply_condition(condition)

    def _set_event_definitions(self,
                               left_event_defs: List[PrimitiveEventDefinition],
                               right_event_defs: List[PrimitiveEventDefinition]):
        """
        A helper function for collecting the event definitions from subtrees.
        """
        self._event_defs = left_event_defs + right_event_defs

    def get_left_subtree(self):
        """
        Returns the left subtree of this node.
        """
        return self._left_subtree

    def get_right_subtree(self):
        """
        Returns the right subtree of this node.
        """
        return self._right_subtree

    def set_subtrees(self, left: Node, right: Node):
        """
        Sets the subtrees of this node.
        """
        self._left_subtree = left
        self._right_subtree = right
        # only the positive children definitions should be applied on this node
        self._set_event_definitions(self._left_subtree.get_positive_event_definitions(),
                                    self._right_subtree.get_positive_event_definitions())

    def _propagate_pattern_parameters(self, pattern_params: PatternParameters):
        self._left_subtree.set_and_propagate_pattern_parameters(pattern_params)
        self._right_subtree.set_and_propagate_pattern_parameters(pattern_params)

    def propagate_pattern_id(self, pattern_id: int):
        self.add_pattern_ids({pattern_id})
        self._left_subtree.propagate_pattern_id(pattern_id)
        self._right_subtree.propagate_pattern_id(pattern_id)

    def replace_subtree(self, old_node: Node, new_node: Node):
        """
        Replaces the child of this node provided as old_node with new_node.
        """
        left = self.get_left_subtree()
        right = self.get_right_subtree()
        if left == old_node:
            self.set_subtrees(new_node, right)
        elif right == old_node:
            self.set_subtrees(left, new_node)
        else:
            raise Exception("old_node must contain one of this node's children")
        new_node.add_parent(self)

    def handle_new_partial_match(self, partial_match_source: Node):
        """
        Internal node's update for a new partial match in one of the subtrees.
        """
        if partial_match_source == self._left_subtree:
            other_subtree = self._right_subtree
        elif partial_match_source == self._right_subtree:
            other_subtree = self._left_subtree
        else:
            raise Exception()  # should never happen

        new_partial_match = partial_match_source.get_last_unhandled_partial_match_by_parent(self)
        new_pm_key = partial_match_source.get_storage_unit().get_key_function()
        first_event_defs = partial_match_source.get_event_definitions_by_parent(self)
        other_subtree.clean_expired_partial_matches(new_partial_match.last_timestamp)
        partial_matches_to_compare = other_subtree.get_partial_matches(new_pm_key(new_partial_match))
        second_event_defs = other_subtree.get_event_definitions_by_parent(self)

        self.clean_expired_partial_matches(new_partial_match.last_timestamp)

        # given a partial match from one subtree, for each partial match
        # in the other subtree we check for new partial matches in this node.
        self._try_create_new_matches(new_partial_match, partial_matches_to_compare, first_event_defs, second_event_defs)

    def _try_create_new_matches(self, new_partial_match: PatternMatch, partial_matches_to_compare: List[PatternMatch],
                                first_event_defs: List[PrimitiveEventDefinition],
                                second_event_defs: List[PrimitiveEventDefinition]):
        """
        For each candidate pair of partial matches that can be joined to create a new one, verifies all the
        necessary conditions creates new partial matches if all constraints are satisfied.
        """
        for partial_match in partial_matches_to_compare:
            events_for_new_match = self._merge_events_for_new_match(first_event_defs, second_event_defs,
                                                                    new_partial_match.events, partial_match.events)
            probability = calculate_joint_probability(new_partial_match.probability, partial_match.probability)
            self._validate_and_propagate_partial_match(events_for_new_match, probability)

    def _merge_events_for_new_match(self,
                                    first_event_defs: List[PrimitiveEventDefinition],
                                    second_event_defs: List[PrimitiveEventDefinition],
                                    first_event_list: List[Event],
                                    second_event_list: List[Event]):
        """
        Creates a list of events to be included in a new partial match.
        """
        if self._event_defs[0].index == first_event_defs[0].index:
            return first_event_list + second_event_list
        if self._event_defs[0].index == second_event_defs[0].index:
            return second_event_list + first_event_list
        raise Exception()

    def is_equivalent(self, other):
        """
        In addition to the checks performed by the base class, checks if:
        the left subtrees structures are equivalent and the right subtrees structures are equivalent OR
        the left of the first is equivalent to the right of the second and the right of the first is equivalent to
        the left of the second.
        """
        if not super().is_equivalent(other):
            return False
        v1 = self._left_subtree.is_equivalent(other.get_left_subtree())
        v2 = self._right_subtree.is_equivalent(other.get_right_subtree())
        if v1 and v2:
            return True
        v3 = self._left_subtree.is_equivalent(other.get_right_subtree())
        v4 = self._right_subtree.is_equivalent(other.get_left_subtree())
        return v3 and v4

    def __get_filtered_conditions(self, left_event_names: Set[str], right_event_names: Set[str]):
        """
        An auxiliary method returning the atomic binary conditions containing variables from the opposite subtrees
        of this node.
        """
        atomic_conditions = self._condition.extract_atomic_conditions()
        filtered_conditions = []
        for atomic_condition in atomic_conditions:
            if not isinstance(atomic_condition, BaseRelationCondition):
                # filtering by condition is only possible for basic relation conditions as of now
                continue
            if not isinstance(atomic_condition.get_left_term(), Variable):
                continue
            if not isinstance(atomic_condition.get_right_term(), Variable):
                continue
            if atomic_condition.get_left_term().name in left_event_names and \
                    atomic_condition.get_right_term().name in right_event_names:
                filtered_conditions.append(atomic_condition)
            elif atomic_condition.get_right_term().name in left_event_names and \
                    atomic_condition.get_left_term().name in right_event_names:
                filtered_conditions.append(atomic_condition)
        return filtered_conditions

    def __get_params_for_sorting_keys(self, conditions: List[BaseRelationCondition], attributes_priorities: dict,
                                      left_event_names: List[str], right_event_names: List[str]):
        """
        An auxiliary method returning the best assignments for the parameters of the sorting keys according to the
        available atomic conditions and user-supplied attribute priorities.
        """
        left_term, left_rel_op, left_equation_size = None, None, None
        right_term, right_rel_op, right_equation_size = None, None, None
        for condition in conditions:
            if condition.get_left_term().name in left_event_names:
                if left_term is None or attributes_priorities[condition.get_left_term().name] > \
                        attributes_priorities[left_term.name]:
                    left_term, left_rel_op, left_equation_size = \
                        condition.get_left_term(), condition.relop_type, EquationSides.left
                if right_term is None or attributes_priorities[condition.get_right_term().name] > \
                        attributes_priorities[right_term.name]:
                    right_term, right_rel_op, right_equation_size = \
                        condition.get_right_term(), condition.relop_type, EquationSides.right
            elif condition.get_left_term().name in right_event_names:
                if left_term is None or attributes_priorities[condition.get_right_term().name] > \
                        attributes_priorities[left_term.name]:
                    left_term, left_rel_op, left_equation_size = \
                        condition.get_right_term(), condition.relop_type, EquationSides.right
                if right_term is None or attributes_priorities[condition.get_left_term().name] > \
                        attributes_priorities[right_term.name]:
                    right_term, right_rel_op, right_equation_size = \
                        condition.get_left_term(), condition.relop_type, EquationSides.left
            else:
                raise Exception("Internal error")
        return left_term, left_rel_op, left_equation_size, right_term, right_rel_op, right_equation_size

    def _get_condition_based_sorting_keys(self, attributes_priorities: dict):
        """
        Calculates the sorting keys according to the conditions in the pattern and the user-provided priorities.
        """
        left_sorting_key, right_sorting_key, rel_op = None, None, None
        left_event_defs = self._left_subtree.get_event_definitions()
        right_event_defs = self._right_subtree.get_event_definitions()
        left_event_names = {item.name for item in left_event_defs}
        right_event_names = {item.name for item in right_event_defs}

        # get the candidate atomic conditions
        filtered_conditions = self.__get_filtered_conditions(left_event_names, right_event_names)
        if len(filtered_conditions) == 0:
            # no conditions to sort according to
            return None, None, None, None, None, None
        if attributes_priorities is None and len(filtered_conditions) > 1:
            # multiple conditions are available, yet the user did not provide a list of priorities
            return None, None, None, None, None, None

        # select the most fitting atomic conditions and assign the respective parameters
        left_term, left_rel_op, left_equation_size, right_term, right_rel_op, right_equation_size = \
            self.__get_params_for_sorting_keys(filtered_conditions, attributes_priorities,
                                               left_event_names, right_event_names)

        # convert terms into sorting key fetching callbacks
        if left_term is not None:
            left_sorting_key = lambda pm: left_term.eval(
                {left_event_defs[i].name: pm.events[i].payload for i in range(len(pm.events))}
            )
        if right_term is not None:
            right_sorting_key = lambda pm: right_term.eval(
                {right_event_defs[i].name: pm.events[i].payload for i in range(len(pm.events))}
            )

        return left_sorting_key, left_rel_op, left_equation_size, right_sorting_key, right_rel_op, right_equation_size
