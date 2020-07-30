from base.Formula import Formula
from base.PatternStructure import PatternStructure, QItem, CompositeStructure
from datetime import timedelta
from misc.StatisticsTypes import StatisticsTypes
from base.PatternStructure import NegationOperator


class Pattern:
    """
    A pattern has several fields:
    - A structure represented by a tree of operators over the primitive events (e.g., SEQ(A,B*, AND(C, NOT(D), E))).
    The entire pattern structure is divided into a positive and a negative component to allow for different treatment
    during evaluation.
    - A condition to be satisfied by the primitive events. This condition might encapsulate multiple nested conditions.
    - A time window for the pattern matches to occur within.
    A pattern can also carry statistics with it, in order to enable advanced
    tree construction mechanisms - this is hopefully a temporary hack.
    """
    def __init__(self, pattern_structure: PatternStructure, pattern_matching_condition: Formula,
                 time_window: timedelta):
        if not isinstance(pattern_structure, CompositeStructure):
            raise Exception("The top pattern operator must be composite")
        self.full_structure = pattern_structure
        self.positive_structure = pattern_structure.duplicate()
        self.negative_structure = self.__extract_negative_structure()

        self.condition = pattern_matching_condition
        self.window = time_window

        self.statistics_type = StatisticsTypes.NO_STATISTICS
        self.statistics = None

    def set_statistics(self, statistics_type: StatisticsTypes, statistics: object):
        self.statistics_type = statistics_type
        self.statistics = statistics

    def __extract_negative_structure(self):
        """
        If the pattern definition includes negative events, this method extracts them into a dedicated
        PatternStructure object. Otherwise, None is returned.
        As of this version, nested negation operators and negation operators in non-flat patterns
        are not supported. Also, the extracted negative events are put in a simple flat positive_structure.
        """
        negative_structure = self.positive_structure.duplicate_top_operator()
        for arg in self.positive_structure.get_args():
            if type(arg) == NegationOperator:
                # a negative event was found and needs to be extracted
                negative_structure.args.append(arg)
            elif type(arg) != QItem:
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
