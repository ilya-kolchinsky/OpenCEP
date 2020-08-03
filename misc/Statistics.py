from typing import List

from base.Formula import Formula
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, QItem
from misc.IOUtils import Stream


def get_condition_selectivity(arg1: QItem, arg2: QItem, formula: Formula, stream: Stream, is_sequence: bool):
    """
    Calculates the selectivity of a given condition between two event types by evaluating it on a given stream.
    """
    if formula is None:
        return 1.0

    count = 0
    match_count = 0

    if arg1 == arg2:
        for event in stream:
            if event.eventType == arg1.type:
                count += 1
                if formula.eval({arg1.name: event.event}):
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
                    if formula.eval({arg1.name: event1.event, arg2.name: event2.event}):
                        match_count += 1
    return match_count / count


def get_occurrences_dict(pattern: Pattern, stream: Stream):
    """
    Returns a dictionary containing the number of occurrences of each event type from the given pattern in the
    given event stream.
    """
    ret = {}
    types = {qitem.eventType for qitem in pattern.structure.args}
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
    args = pattern.structure.args
    args_num = len(args)
    selectivity_matrix = [[0.0 for _ in range(args_num)] for _ in range(args_num)]
    for i in range(args_num):
        for j in range(i + 1):
            new_sel = get_condition_selectivity(args[i], args[j],
                                                pattern.condition.get_formula_of({args[i].name, args[j].name}),
                                                stream.duplicate(), pattern.structure.get_top_operator() == SeqOperator)
            selectivity_matrix[i][j] = selectivity_matrix[j][i] = new_sel

    return selectivity_matrix


def get_arrival_rates(pattern: Pattern, stream: Stream):
    """
    Returns a list containing the arrival rates of the event types defined by the given pattern, measured according to
    their appearances in given event stream.
    """
    time_interval = (stream.last().date - stream.first().date).total_seconds()
    counters = get_occurrences_dict(pattern, stream.duplicate())
    return [counters[i.eventType] / time_interval for i in pattern.structure.args]


def calculate_left_deep_tree_cost_function(order: List[int], selectivity_matrix: List[List[float]],
                                           arrival_rates: List[int], time_window: int):
    """
    Calculates the cost function of a left-deep tree specified by the given order.
    """
    cost = 0
    to_add = 1
    for i in range(len(order)):
        to_add *= selectivity_matrix[order[i]][order[i]] * arrival_rates[order[i]] * time_window
        for j in range(i):
            to_add *= selectivity_matrix[order[i]][order[j]]
        cost += to_add
    return cost


def calculate_bushy_tree_cost_function(tree: tuple or int, selectivity_matrix: List[List[float]],
                                       arrival_rates: List[int], time_window: int):
    """
    Calculates the cost function of the given tree.
    """
    _, _, cost = calculate_bushy_tree_cost_function_helper(tree, selectivity_matrix, arrival_rates, time_window)
    return cost


def calculate_bushy_tree_cost_function_helper(tree: tuple or int, selectivity_matrix: List[List[float]],
                                              arrival_rates: List[int], time_window: int):
    """
    A helper function for calculating the cost function of the given tree.
    """
    # calculate base case: tree is a leaf.
    if type(tree) == int:
        cost = pm = time_window * arrival_rates[tree] * selectivity_matrix[tree][tree]
        return [tree], pm, cost

    # calculate for left subtree
    left_args, left_pm, left_cost = calculate_bushy_tree_cost_function_helper(tree[0], selectivity_matrix,
                                                                              arrival_rates, time_window)
    # calculate for right subtree
    right_args, right_pm, right_cost = calculate_bushy_tree_cost_function_helper(tree[1], selectivity_matrix,
                                                                                 arrival_rates, time_window)
    # calculate from left and right subtrees for this subtree.
    pm = left_pm * right_pm
    for left_arg in left_args:
        for right_arg in right_args:
            pm *= selectivity_matrix[left_arg][right_arg]
    cost = left_cost + right_cost + pm
    return left_args + right_args, pm, cost


class MissingStatisticsException(Exception):
    pass
