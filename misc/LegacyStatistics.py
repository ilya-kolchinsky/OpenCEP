from condition.Condition import Condition
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, PrimitiveEventStructure
from stream.Stream import Stream


def get_condition_selectivity(arg1: PrimitiveEventStructure, arg2: PrimitiveEventStructure, condition: Condition,
                              stream: Stream, is_sequence: bool):
    """
    Calculates the selectivity of a given condition between two event types by evaluating it on a given stream.
    """
    if condition is None:
        return 1.0

    count = 0
    match_count = 0

    if arg1 == arg2:
        for event in stream:
            if event.eventType == arg1.type:
                count += 1
                if condition.eval({arg1.name: event.event}):
                    match_count += 1
    else:
        events1 = []
        events2 = []
        for event in stream:
            if event.eventType == arg1.type:
                events1.append(event)
            elif event.eventType == arg2.type:
                events2.append(event)
        for event1 in events1:
            for event2 in events2:
                if (not is_sequence) or event1.date < event2.date:
                    count += 1
                    if condition.eval({arg1.name: event1.event, arg2.name: event2.event}):
                        match_count += 1
    return match_count / count


def get_occurrences_dict(pattern: Pattern, stream: Stream):
    """
    Returns a dictionary containing the number of occurrences of each event type from the given pattern in the
    given event stream.
    """
    ret = {}
    types = {primitive_event.eventType for primitive_event in pattern.positive_structure.args}
    for event in stream:
        if event.eventType in types:
            if event.eventType in ret.keys():
                ret[event.eventType] += 1
            else:
                ret[event.eventType] = 1
    return ret


def calculate_selectivity_matrix(pattern: Pattern, stream: Stream):
    """
    Returns a matrix containing the selectivity between each pair of events from the given pattern in the
    given event stream.
    """
    args = pattern.positive_structure.args
    args_num = len(args)
    selectivity_matrix = [[0.0 for _ in range(args_num)] for _ in range(args_num)]
    for i in range(args_num):
        for j in range(i + 1):
            new_sel = get_condition_selectivity(args[i], args[j],
                                                pattern.condition.get_condition_of({args[i].name, args[j].name}),
                                                stream.duplicate(), pattern.positive_structure.get_top_operator() == SeqOperator)
            selectivity_matrix[i][j] = selectivity_matrix[j][i] = new_sel

    return selectivity_matrix


def get_arrival_rates(pattern: Pattern, stream: Stream):
    """
    Returns a list containing the arrival rates of the event types defined by the given pattern, measured according to
    their appearances in given event stream.
    """
    time_interval = (stream.last().date - stream.first().date).total_seconds()
    counters = get_occurrences_dict(pattern, stream.duplicate())
    return [counters[i.eventType] / time_interval for i in pattern.positive_structure.args]


class MissingStatisticsException(Exception):
    pass
