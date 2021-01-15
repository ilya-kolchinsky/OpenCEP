from functools import reduce
from typing import List

from base.Event import Event
from condition.Condition import Condition, Variable, BinaryCondition, TrueCondition
from condition.CompositeCondition import CompositeCondition, AndCondition
from base.PatternStructure import PatternStructure, CompositeStructure, PrimitiveEventStructure, \
    SeqOperator, NegationOperator, KleeneClosureOperator
from datetime import timedelta
from misc.StatisticsTypes import StatisticsTypes
from misc.ConsumptionPolicy import ConsumptionPolicy
from plan.NegationAlgorithmTypes import NegationAlgorithmTypes


class Pattern:
    """
    A pattern has several fields:
    - A structure represented by a tree of operators over the primitive events (e.g., SEQ(A,B*, AND(C, NOT(D), E))).
    The entire pattern structure is divided into a positive and a negative component to allow for different treatment
    during evaluation.
    - A negation algorithm (chosen by the user)
    - A mapping from event name to its index (necessary for the lowest pos negation algorithm)
    - A condition to be satisfied by the primitive events. This condition might encapsulate multiple nested conditions.
    - A time window for the pattern matches to occur within.
    - A ConsumptionPolicy object that contains the policies that filter certain partial matches.
    A pattern can also carry statistics with it, in order to enable advanced
    tree construction mechanisms - this is hopefully a temporary hack.
    """
    def __init__(self, pattern_structure: PatternStructure, pattern_matching_condition: Condition,
                 time_window: timedelta, consumption_policy: ConsumptionPolicy = None, pattern_id: int = None):
        self.id = pattern_id
        self.full_structure = pattern_structure
        self.positive_structure = pattern_structure.duplicate()
        self.negative_structure = self.__extract_negative_structure()
        self.negation_algorithm = None
        self.name_idx_events_map = self.create_name_idx_map()
        self.condition = pattern_matching_condition
        if isinstance(self.condition, TrueCondition):
            self.condition = AndCondition()
        elif not isinstance(self.condition, CompositeCondition):
            self.condition = AndCondition(self.condition)

        self.window = time_window

        self.statistics_type = StatisticsTypes.NO_STATISTICS
        self.full_statistics = None
        # "statistics" includes only the statistics of the positive events
        self.statistics = None
        self.consumption_policy = consumption_policy

        if consumption_policy is not None:
            if consumption_policy.single_event_strategy is not None and consumption_policy.single_types is None:
                # must be initialized to contain all event types in the pattern
                consumption_policy.single_types = self.get_all_event_types()
            if consumption_policy.contiguous_names is not None:
                self.__init_strict_conditions(pattern_structure)

    def create_name_idx_map(self):
        """
        Creates a mapping from event name to its fake index (the index in the positive structure or the negative one)
        """
        if type(self.full_structure) != KleeneClosureOperator:
            args = self.positive_structure.get_args()
            if self.negative_structure is not None:
                args = self.positive_structure.get_args() + self.negative_structure.get_args()
            name_idx_map = {}
            for idx, arg in enumerate(args):
                if type(arg) == NegationOperator:
                    name_idx_map[arg.arg.name] = idx
                elif type(arg) == PrimitiveEventStructure:
                    name_idx_map[arg.name] = idx
            return name_idx_map

    def set_negation_algorithm(self, neg_alg: NegationAlgorithmTypes):
        """
        Sets the negation algorithm that was chosen by the user
        """
        if self.negative_structure is not None:
            self.negation_algorithm = neg_alg if neg_alg is not None\
                else NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM

    def set_statistics(self, statistics_type: StatisticsTypes, statistics: object):
        """
        Sets the statistical properties related to the events and conditions of this pattern.
        """
        self.statistics_type = statistics_type
        self.full_statistics = statistics
        if statistics_type not in [StatisticsTypes.ARRIVAL_RATES, StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES]:
            self.statistics = statistics
            return
        positive_selectivity = []
        positive_arrival = []
        positive_indices = []
        for i in range(0, len(self.full_structure.args)):
            if type(self.full_structure.args[i]) is not NegationOperator:
                positive_indices.append(i)
        for i in positive_indices:
            if statistics_type is StatisticsTypes.ARRIVAL_RATES:
                positive_arrival.append(statistics[i])
            else:
                positive_selectivity.append([(statistics[0][i])[j] for j in positive_indices])
                positive_arrival.append(statistics[1][i])
        self.statistics = positive_arrival if statistics_type is StatisticsTypes.ARRIVAL_RATES else\
            (positive_selectivity, positive_arrival)

    def __extract_negative_structure(self):
        """
        If the pattern definition includes negative events, this method extracts them into a dedicated
        PatternStructure object. Otherwise, None is returned.
        As of this version, nested negation operators and negation operators in non-flat patterns
        are not supported. Also, the extracted negative events are put in a simple flat positive_structure.
        """
        if not isinstance(self.positive_structure, CompositeStructure):
            # Cannot contain a negative part
            return None
        negative_structure = self.positive_structure.duplicate_top_operator()
        for idx, arg in enumerate(self.positive_structure.get_args()):
            if type(arg) == NegationOperator:
                # A negative event was found and needs to be extracted
                arg.arg.set_orig_idx(idx)
                negative_structure.args.append(arg)
            elif type(arg) != PrimitiveEventStructure:
                # TODO: nested operator support should be provided here
                pass
            elif type(arg) == PrimitiveEventStructure:
                arg.set_orig_idx(idx)

        if len(negative_structure.get_args()) == 0:
            # The given pattern is entirely positive
            return None
        if len(negative_structure.get_args()) == len(self.positive_structure.get_args()):
            raise Exception("The pattern contains no positive events")
        # Remove all negative events from the positive structure
        # TODO: support nested operators
        for arg in negative_structure.get_args():
            self.positive_structure.get_args().remove(arg)
        return negative_structure

    def update_conds_lists(self):
        """
        Updates the "pos_related_idxs" lists of all the negation operators (all the args of the negative structure)
        with the indices of the positive events they have condition with.
        """
        num_of_pos = len(self.positive_structure.get_args())
        for cond in self.condition.get_conditions_list():
            neg_idxs = set()
            pos_idxs = set()
            for term in cond.terms:
                term_idx = self.name_idx_events_map[term.name]
                if term_idx >= num_of_pos:
                    neg_idxs.add(term_idx-num_of_pos)
                else:
                    pos_idxs.add(term_idx)
            for neg_idx in neg_idxs:
                self.negative_structure.get_args()[neg_idx].pos_related_idxs.update(pos_idxs)

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

    def __repr__(self):
        return "\nPattern structure: %s\nCondition: %s\nTime window: %s\n\n" % (self.structure,
                                                                                self.condition,
                                                                                self.window)
