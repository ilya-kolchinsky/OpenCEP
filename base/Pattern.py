from functools import reduce
from typing import List

from base.Event import Event
from base.Formula import Formula, EqFormula, IdentifierTerm, MinusTerm, AtomicTerm, AndFormula
from base.PatternStructure import PatternStructure, CompositeStructure, SeqOperator, \
    PrimitiveEventStructure, NegationOperator
from datetime import timedelta
from misc.StatisticsTypes import StatisticsTypes
from misc.ConsumptionPolicy import ConsumptionPolicy


class Pattern:
    """
    A pattern has several fields:
    - A structure represented by a tree of operators over the primitive events (e.g., SEQ(A,B*, AND(C, NOT(D), E))).
    The entire pattern structure is divided into a positive and a negative component to allow for different treatment
    during evaluation.
    - A condition to be satisfied by the primitive events. This condition might encapsulate multiple nested conditions.
    - A time window for the pattern matches to occur within.
    - a ConsumptionPolicy object that contains the policies that filter certain partial matches.
    A pattern can also carry statistics with it, in order to enable advanced
    tree construction mechanisms - this is hopefully a temporary hack.
    """
    def __init__(self, pattern_structure: PatternStructure, pattern_matching_condition: Formula,
                 time_window: timedelta, consumption_policy: ConsumptionPolicy = None, pattern_id: int = None):
        self.id = pattern_id

        self.full_structure = pattern_structure
        self.positive_structure = pattern_structure.duplicate()
        self.negative_structure = self.__extract_negative_structure()

        self.condition = pattern_matching_condition
        self.window = time_window

        self.statistics_type = StatisticsTypes.NO_STATISTICS
        self.statistics = None
        self.consumption_policy = consumption_policy

        if consumption_policy is not None:
            if consumption_policy.single_event_strategy is not None and consumption_policy.single_types is None:
                # must be initialized to contain all event types in the pattern
                consumption_policy.single_types = self.get_all_event_types()
            if consumption_policy.contiguous_names is not None:
                self.__init_strict_formulas(pattern_structure)

    def set_statistics(self, statistics_type: StatisticsTypes, statistics: object):
        """
        Sets the statistical properties related to the events and conditions of this pattern.
        """
        self.statistics_type = statistics_type
        self.statistics = statistics

    def __extract_negative_structure(self):
        """
        If the pattern definition includes negative events, this method extracts them into a dedicated
        PatternStructure object. Otherwise, None is returned.
        As of this version, nested negation operators and negation operators in non-flat patterns
        are not supported. Also, the extracted negative events are put in a simple flat positive_structure.
        """
        if not isinstance(self.positive_structure, CompositeStructure):
            # cannot contain a negative part
            return None
        negative_structure = self.positive_structure.duplicate_top_operator()
        for arg in self.positive_structure.get_args():
            if type(arg) == NegationOperator:
                # a negative event was found and needs to be extracted
                negative_structure.args.append(arg)
            elif type(arg) != PrimitiveEventStructure:
                # TODO: nested operator support should be provided here
                pass
        if len(negative_structure.get_args()) == 0:
            # the given pattern is entirely positive
            return None
        if len(negative_structure.get_args()) == len(self.positive_structure.get_args()):
            raise Exception("The pattern contains no positive events")
        # Remove all negative events from the positive structure
        # TODO: support nested operators
        for arg in negative_structure.get_args():
            self.positive_structure.get_args().remove(arg)
        return negative_structure

    def get_index_by_event_name(self, event_name: str):
        """
        Returns the position of the given event name in the pattern.
        Note: nested patterns are not yet supported.
        """
        found_positions = [index for (index, curr_structure) in enumerate(self.full_structure.get_args())
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
        return reduce(lambda x, y: x+y, [self.__get_all_event_types_aux(arg) for arg in structure.args])

    def __init_strict_formulas(self, pattern_structure: PatternStructure):
        """
        Augment the pattern with the contiguity constraints specified as a part of the consumption policy.
        """
        if not isinstance(pattern_structure, CompositeStructure):
            return
        args = pattern_structure.args
        for i in range(len(args)):
            self.__init_strict_formulas(args[i])
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
        self.condition = AndFormula(
            self.condition,
            EqFormula(
                IdentifierTerm(first_name, lambda x: x[Event.INDEX_ATTRIBUTE_NAME]),
                MinusTerm(IdentifierTerm(second_name, lambda x: x[Event.INDEX_ATTRIBUTE_NAME]), AtomicTerm(1))
            )
        )

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

    def __repr__(self):
        return "\nPattern structure: %s\nCondition: %s\nTime window: %s\n\n" % (self.structure,
                                                                                self.condition,
                                                                                self.window)
