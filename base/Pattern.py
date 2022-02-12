from functools import reduce
from typing import List, Dict

from adaptive.statistics.StatisticsTypes import StatisticsTypes
from base.Event import Event
from condition.Condition import Condition, Variable, BinaryCondition, TrueCondition
from condition.CompositeCondition import CompositeCondition, AndCondition
from base.PatternStructure import PatternStructure, CompositeStructure, PrimitiveEventStructure, \
    SeqOperator, NegationOperator, UnaryStructure
from datetime import timedelta
from misc.ConsumptionPolicy import ConsumptionPolicy


class Pattern:
    """
    A pattern has several fields:
    - A structure represented by a tree of operators over the primitive events (e.g., SEQ(A,B*, AND(C, NOT(D), E))).
    The entire pattern structure is divided into a positive and a negative component to allow for different treatment
    during evaluation.
    - A condition to be satisfied by the primitive events. This condition might encapsulate multiple nested conditions.
    - A time window for the pattern matches to occur within.
    - A ConsumptionPolicy object that contains the policies that filter certain partial matches.
    - An optional confidence parameter, intended to indicate the minimal acceptable probability of a pattern match. This
    parameter is only applicable for probabilistic data streams.
    A pattern can also carry statistics with it, in order to enable advanced tree construction mechanisms - this is
    hopefully a temporary hack.
    """
    def __init__(self, pattern_structure: PatternStructure, pattern_matching_condition: Condition,
                 time_window: timedelta, consumption_policy: ConsumptionPolicy = None, pattern_id: int = None,
                 confidence: float = None, statistics: Dict = None):
        if confidence is not None and (confidence < 0.0 or confidence > 1.0):
            raise Exception("Invalid value for pattern confidence:%s" % (confidence,))
        self.id = pattern_id

        self.full_structure = pattern_structure
        self.positive_structure = pattern_structure.duplicate()
        self.negative_structure = self.__extract_negative_structure()

        self.condition = pattern_matching_condition
        if isinstance(self.condition, TrueCondition) or self.condition is None:
            self.condition = AndCondition()
        elif not isinstance(self.condition, CompositeCondition):
            self.condition = AndCondition(self.condition)

        self.window = time_window

        self.statistics = statistics

        self.consumption_policy = consumption_policy
        if consumption_policy is not None:
            if consumption_policy.single_event_strategy is not None and consumption_policy.single_types is None:
                # must be initialized to contain all event types in the pattern
                consumption_policy.single_types = self.get_all_event_types()
            if consumption_policy.contiguous_names is not None:
                self.__init_strict_conditions(pattern_structure)

        self.confidence = confidence

    def set_statistics(self, statistics: Dict):
        """
        Sets the statistical properties related to the events and conditions of this pattern.
        """
        self.statistics = statistics

    def __extract_negative_structure(self):
        """
        If the pattern definition includes negative events, this method extracts them into a dedicated
        PatternStructure object. Otherwise, None is returned.
        Note that this method only extracts the non-nested negative structure of the current nesting level. That is,
        nested negation operators and negation operators in non-flat patterns are ignored.
        """
        if not isinstance(self.positive_structure, CompositeStructure):
            # cannot contain a negative part
            return None
        negative_structure = self.positive_structure.duplicate_top_operator()
        for arg in self.positive_structure.args:
            if type(arg) == NegationOperator:
                # a negative event was found and needs to be extracted
                negative_structure.args.append(arg)
        if len(negative_structure.args) == 0:
            # the given pattern is entirely positive
            return None
        if len(negative_structure.args) == len(self.positive_structure.args):
            raise Exception("The pattern contains no positive events")
        # Remove all negative events from the positive structure
        for arg in negative_structure.args:
            self.positive_structure.args.remove(arg)
        return negative_structure

    def get_index_by_event_name(self, event_name: str):
        """
        Returns the position of the given event name in the pattern.
        Note: nested patterns are not yet supported.
        """
        found_positions = [index for (index, curr_structure) in enumerate(self.full_structure.args)
                           if curr_structure.contains_event(event_name)]
        if len(found_positions) == 0:
            raise Exception("Event name %s not found in pattern" % (event_name,))
        if len(found_positions) > 1:
            raise Exception("Multiple appearances of the event name %s are found in the pattern" % (event_name,))
        return found_positions[0]

    def get_all_event_types(self):
        """
        Returns all event types in the pattern.
        """
        return set(self.__get_all_event_types_aux(self.full_structure))

    def __get_all_event_types_aux(self, structure: PatternStructure):
        """
        An auxiliary method for returning all event types in the pattern.
        """
        if isinstance(structure, PrimitiveEventStructure):
            return [structure.type]
        if isinstance(structure, UnaryStructure):
            return self.__get_all_event_types_aux(structure.arg)
        return reduce(lambda x, y: x + y, [self.__get_all_event_types_aux(arg) for arg in structure.args])

    def get_primitive_events(self) -> List[PrimitiveEventStructure]:
        """
        Returns a list of primitive events that make up the pattern structure.
        """
        if isinstance(self.full_structure, UnaryStructure):
            full_structure_args = self.full_structure.arg
        else:
            full_structure_args = self.full_structure.args
        primitive_events = self.__get_primitive_events_aux(full_structure_args)
        # a hack to remove unhashable duplicates from a list.
        return list({str(x): x for x in primitive_events}.values())

    def __get_primitive_events_aux(self, pattern_args) -> List[PrimitiveEventStructure]:
        """
        An auxiliary method for returning a list of primitive events composing the pattern structure.
        """
        primitive_events = []
        while not isinstance(pattern_args, List) and not isinstance(pattern_args, PrimitiveEventStructure):
            if isinstance(pattern_args, UnaryStructure):
                pattern_args = pattern_args.arg
            else:
                pattern_args = pattern_args.args
        if isinstance(pattern_args, PrimitiveEventStructure):
            primitive_events.append(pattern_args)
        else:
            for event in pattern_args:
                if isinstance(event, PrimitiveEventStructure):
                    primitive_events.append(event)
                else:
                    primitive_events.extend(self.__get_primitive_events_aux(event))
        return primitive_events

    def __init_strict_conditions(self, pattern_structure: PatternStructure):
        """
        Augment the pattern with the contiguity constraints specified as a part of the consumption policy.
        """
        if not isinstance(pattern_structure, CompositeStructure):
            return
        args = pattern_structure.args
        for i in range(len(args)):
            self.__init_strict_conditions(args[i])
        if pattern_structure.get_top_operator() != SeqOperator:
            return
        for contiguous_sequence in self.consumption_policy.contiguous_names:
            for i in range(len(contiguous_sequence) - 1):
                for j in range(len(args) - 1):
                    if not isinstance(args[i], PrimitiveEventStructure) or \
                            not isinstance(args[i + 1], PrimitiveEventStructure):
                        continue
                    if contiguous_sequence[i] != args[j].name:
                        continue
                    if contiguous_sequence[i + 1] != args[j + 1].name:
                        raise Exception("Contiguity constraints contradict the pattern structure: " +
                                        "%s must follow %s" % (contiguous_sequence[i], contiguous_sequence[i + 1]))
                    self.__add_contiguity_condition(args[i].name, args[i + 1].name)

    def __add_contiguity_condition(self, first_name: str, second_name: str):
        """
        Augment the pattern condition with a contiguity constraint between the given event names.
        """
        contiguity_condition = BinaryCondition(Variable(first_name, lambda x: x[Event.INDEX_ATTRIBUTE_NAME]),
                                               Variable(second_name, lambda x: x[Event.INDEX_ATTRIBUTE_NAME]),
                                               lambda x, y: x == y - 1)
        self.condition.add_atomic_condition(contiguity_condition)

    def extract_flat_sequences(self) -> List[List[str]]:
        """
        Returns a list of all flat sequences in the pattern.
        For now, nested operators inside the scope of a sequence are not supported. For example,
        in the pattern SEQ(A,AND(B,C),D) there are two hidden sequences [A,B,D] and [A,C,D], but this method will
        not return them as of now.
        """
        return self.__extract_flat_sequences_aux(self.positive_structure)

    def __extract_flat_sequences_aux(self, pattern_structure: PatternStructure) -> List[List[str]] or None:
        """
        An auxiliary method for extracting flat sequences from the pattern.
        """
        if isinstance(pattern_structure, PrimitiveEventStructure):
            return None
        if pattern_structure.get_top_operator() == SeqOperator:
            # note the double brackets - we return a list composed of a single list representing this sequence
            return [[arg.name for arg in pattern_structure.args if isinstance(arg, PrimitiveEventStructure)]]
        # the pattern is a composite pattern but not a flat sequence
        result = []
        for arg in pattern_structure.args:
            nested_sequences = self.__extract_flat_sequences_aux(arg)
            if nested_sequences is not None:
                result.extend(nested_sequences)
        return result

    def count_primitive_events(self, positive_only=False, negative_only=False):
        """
        Returns the total number of primitive events in this pattern.
        """
        return len(self.get_primitive_event_names(positive_only=positive_only, negative_only=negative_only))

    def get_primitive_event_names(self, positive_only=False, negative_only=False):
        """
        Returns all the event names of primitive events in this pattern.
        """
        if positive_only and negative_only:
            raise Exception("Wrong method usage")
        if positive_only:
            return self.positive_structure.get_all_event_names()
        if negative_only:
            return self.negative_structure.get_all_event_names()
        return self.full_structure.get_all_event_names()

    def get_top_level_structure_args(self, positive_only=False, negative_only=False):
        """
        Returns the highest-level arguments of the pattern structure.
        """
        if positive_only and negative_only:
            raise Exception("Wrong method usage")
        if positive_only:
            target_structure = self.positive_structure
        elif negative_only:
            target_structure = self.negative_structure
        else:
            target_structure = self.full_structure
        if isinstance(target_structure, UnaryStructure):
            return [target_structure.arg]
        if isinstance(target_structure, CompositeStructure):
            return target_structure.args
        raise Exception("Invalid top-level pattern structure")

    def get_sub_pattern(self, event_names: List[str]):
        """
        Given a list of event names, return the sub pattern consisting of these events.
        """
        structure = self.full_structure.get_structure_projection(event_names)
        if structure is None:
            return None
        conditions = self.condition.get_condition_projection(event_names)
        sub_pattern = Pattern(pattern_structure=structure, pattern_matching_condition=conditions,
                       time_window=self.window, consumption_policy=self.consumption_policy,
                       confidence=self.confidence)
        # Create statistics that include only data about the sub pattern
        modified_statistics = self.create_modified_statistics(self.statistics, sub_pattern)
        sub_pattern.set_statistics(modified_statistics)
        return sub_pattern

    def is_sub_pattern(self, other_pattern):
        """
        Check if the current pattern (self) is a subpattern of other_pattern.
        """
        if other_pattern is None:
            return False
        current_pattern_events = self.get_primitive_event_names()
        real_subpattern = other_pattern.get_sub_pattern(current_pattern_events)
        if real_subpattern is None:
            return False
        return self == real_subpattern

    def create_modified_statistics(self, original_statistics: Dict, modified_pattern):
        """
        Create a statistics dict for modified_pattern based on the original pattern.
        modified_pattern is a variation of the original pattern (self).
        For example, the original pattern could be SEQ(A,B,C), and modified_pattern could be SEQ(B,C,A).
        """
        if original_statistics is None or len(original_statistics) == 0:
            return original_statistics
        # this dict keeps a mapping between the order of the event in the original pattern to the event name
        original_index_to_event_mapping = {index: event_name for index, event_name
                                           in enumerate(self.get_primitive_event_names())}
        # this dict keeps a mapping between the event name to its order in the modified pattern
        modified_event_to_index_mapping = {event_name: index for index, event_name
                                           in enumerate(modified_pattern.get_primitive_event_names())}
        # using both dicts above we will reorder the statistics according to the modified pattern order
        return self.__aux_create_modified_statistics(original_statistics, original_index_to_event_mapping,
                                                                modified_event_to_index_mapping)

    def __aux_create_modified_statistics(self, original_statistics: Dict, original_index_to_event_mapping: Dict,
                                     modified_event_to_index_mapping: Dict):
        """
        Inner function of create_modified_statistics.
        """
        new_statistics = dict()

        original_arrival = original_statistics.get(StatisticsTypes.ARRIVAL_RATES)
        if original_arrival is not None:
            new_arrival = [0 for i in modified_event_to_index_mapping]
            # fill arrival rate
            for old_index, event_name in original_index_to_event_mapping.items():
                new_index = modified_event_to_index_mapping.get(event_name)
                if new_index is not None:
                    new_arrival[new_index] = original_arrival[old_index]
            new_statistics[StatisticsTypes.ARRIVAL_RATES] = new_arrival

        original_selectivity = original_statistics.get(StatisticsTypes.SELECTIVITY_MATRIX)
        if original_selectivity is not None:
            new_selectivity = [[0 for j in modified_event_to_index_mapping] for i in modified_event_to_index_mapping]

            # fill selectivity matrix
            for i, first_event_name in original_index_to_event_mapping.items():
                new_row_index = modified_event_to_index_mapping.get(first_event_name)
                if new_row_index is None:
                    continue
                for j, second_event_name in original_index_to_event_mapping.items():
                    new_column_index = modified_event_to_index_mapping.get(second_event_name)
                    if new_column_index is None:
                        continue
                    new_selectivity[new_row_index][new_column_index] = original_selectivity[i][j]
            new_statistics[StatisticsTypes.SELECTIVITY_MATRIX] = new_selectivity

        return new_statistics

    def __repr__(self):
        return "\nPattern structure: %s\nCondition: %s\nTime window: %s\n\n" % (self.full_structure,
                                                                                self.condition,
                                                                                self.window)

    def __eq__(self, other):
        return id(self) == id(other) or (self.full_structure == other.full_structure and
                                         self.condition == other.condition)

    def __hash__(self):
        return hash(str(self))
