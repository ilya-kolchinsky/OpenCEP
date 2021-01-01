import itertools
from datetime import timedelta

from typing import List

from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, PrimitiveEventStructure, NegationOperator, AndOperator
from condition.BaseRelationCondition import GreaterThanCondition, SmallerThanCondition, SmallerThanEqCondition, \
    GreaterThanEqCondition
from condition.CompositeCondition import AndCondition
from condition.Condition import Variable
from plan.DeepTreeBuilder import DeepTreeBuilder
from plan.TreePlanBuilder import TreePlanBuilder
from plan.TreePlanBuilderFactory import TreePlanBuilderFactory
from plan.TreePlanBuilderOrders import TreePlanBuilderOrder
from plan.multi.MultiPatternUnifiedTreePlanApproaches import MultiPatternTreePlanUnionApproaches
from test.testUtils import *

# lambda functions often used in tests
get_opening_price = lambda x: x["Opening Price"]
get_peak_price = lambda x: x["Peak Price"]
get_lowest_price = lambda x: x["Lowest Price"]
approaches = {
    1: TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
    2: TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE,
    3: TreePlanBuilderTypes.GREEDY_LEFT_DEEP_TREE,
    4: TreePlanBuilderTypes.LOCAL_SEARCH_LEFT_DEEP_TREE,
    5: TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE,
    6: TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
    7: TreePlanBuilderTypes.ZSTREAM_BUSHY_TREE,
    8: TreePlanBuilderTypes.ORDERED_ZSTREAM_BUSHY_TREE}


def split(string):
    return '{:10s}'.format(str(string).split(".")[1].split("_TREE")[0].replace("_", " "))


# def get_size_of_intersection(pattern_1: Pattern, pattern_2: Pattern, approach):
#     builder = DeepTreeBuilder(tree_plan_order_approach=approach)
#     multi_pattern_eval_approach = MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION
#     tree1 = builder.build_tree_plan(pattern_1)
#     tree2 = builder.build_tree_plan(pattern_2)
#     pattern_to_tree_plan_map = {pattern_1: tree1, pattern_2: tree2}
#     size_of_intersection = builder._union_tree_plans(pattern_to_tree_plan_map.copy(),
#                                                      multi_pattern_eval_approach).trees_number_nodes_shared
#     return size_of_intersection

def myAssert(output: int, expected: int, approach: int):
    print("SUCCESS" + str(approach)) if output == expected else print("FAILED" + str(approach))


def get_max_size_of_intersection_of_all_patterns(patterns: List[Pattern],
                                                 approach: MultiPatternTreePlanUnionApproaches):
    eval_mechanism_params = TreeBasedEvaluationMechanismParameters()
    tree_plan_builder = TreePlanBuilderFactory.create_tree_plan_builder(eval_mechanism_params.tree_plan_params)
    pattern_to_tree_plan_map = {pattern: tree_plan_builder.build_tree_plan(pattern) for pattern in patterns}
    builder = DeepTreeBuilder()
    _ = builder._union_tree_plans(pattern_to_tree_plan_map.copy(),
                                  approach)
    size_of_intersection = builder.trees_number_nodes_shared
    return size_of_intersection


# tests for sharing leaves

def same_leaves_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("GOOG", "e")),
        AndCondition(
            GreaterThanCondition(Variable("a", get_opening_price),
                                 Variable("b", get_opening_price)),
            GreaterThanCondition(Variable("c", get_peak_price), 503)
        ),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("GOOG", "e")),
        AndCondition(
            GreaterThanCondition(Variable("a", get_opening_price),
                                 Variable("b", get_opening_price)),
            GreaterThanCondition(Variable("c", get_peak_price), 503)
        ),
        timedelta(minutes=5)
    )
    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES),
             4, 1)
    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION),
             7, 2)


def same_eventsType_different_names_leaves_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("GOOG", "e")),
        AndCondition(
            GreaterThanCondition(Variable("a", get_opening_price),
                                 Variable("b", get_opening_price)),
            GreaterThanCondition(Variable("c", get_peak_price), 503)
        ),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "m"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("GOOG", "e")),
        AndCondition(
            GreaterThanCondition(Variable("m", get_opening_price),
                                 Variable("b", get_opening_price)),
            GreaterThanCondition(Variable("c", get_peak_price), 503)
        ),
        timedelta(minutes=5)
    )
    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES),
             3, 1)
    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION),
             3, 2)


def same_events_different_condition_leaves_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("GOOG", "e")),
        AndCondition(
            GreaterThanCondition(Variable("a", get_opening_price),
                                 Variable("b", get_opening_price)),
            GreaterThanCondition(Variable("c", get_peak_price), 503)
        ),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("GOOG", "e")),
        AndCondition(
            GreaterThanCondition(Variable("a", get_opening_price),
                                 Variable("b", get_opening_price)),
            GreaterThanCondition(Variable("c", get_peak_price), 500)
        ),
        timedelta(minutes=5)
    )
    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES),
             3, 1)
    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION),
             4, 2)


def sameNames_different_eventTypes_leaves_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("GOOG", "e")),
        AndCondition(
            GreaterThanCondition(Variable("a", get_opening_price),
                                 Variable("b", get_opening_price)),
            GreaterThanCondition(Variable("c", get_peak_price), 500)
        ),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("AMZN", "e")),  # no the same event as the first pattern
        AndCondition(
            GreaterThanCondition(Variable("a", get_opening_price),
                                 Variable("b", get_opening_price)),
            GreaterThanCondition(Variable("c", get_peak_price), 500)
        ),
        timedelta(minutes=5)
    )
    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES),
             3, 1)
    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION),
             5, 2)


def same_events_different_function_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("GOOG", "e")),
        AndCondition(
            GreaterThanCondition(Variable("a", get_opening_price), 5),
            GreaterThanCondition(Variable("c", get_peak_price), 500)
        ),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("GOOG", "e")),
        AndCondition(
            GreaterThanCondition(Variable("a", get_opening_price), 7),  # different function from pattern 1
            GreaterThanCondition(Variable("c", get_peak_price), 500)
        ),
        timedelta(minutes=5)
    )
    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES),
             3, 1)
    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION),
             3, 2)


def same_leaves_different_time_stamps_leaves_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanEqCondition(Variable("a", get_peak_price), 135),
            SmallerThanCondition(Variable("b", get_peak_price),
                                 Variable("c", get_peak_price))
        ),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanEqCondition(Variable("a", get_peak_price), 135),
            SmallerThanCondition(Variable("b", get_peak_price),
                                 Variable("c", get_peak_price))
        ),
        timedelta(minutes=2)
    )

    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES),
             3, 1)
    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION),
             5, 2)


def distinct_leaves_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"),
                    PrimitiveEventStructure("GOOG", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", get_peak_price),
                                 Variable("b", get_peak_price)),
            SmallerThanCondition(Variable("b", get_peak_price),
                                 Variable("c", get_peak_price))
        ),
        timedelta(minutes=3)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "x1"),
                    PrimitiveEventStructure("AMZN", "x2"),
                    PrimitiveEventStructure("AMZN", "x3")),
        AndCondition(
            SmallerThanEqCondition(Variable("x1", get_lowest_price), 75),
            GreaterThanEqCondition(Variable("x2", get_peak_price), 78),
            SmallerThanEqCondition(Variable("x3", get_lowest_price),
                                   Variable("x1", get_lowest_price))
        ),
        timedelta(days=1)
    )
    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES),
             0, 1)
    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION),
             0, 2)


def partially_shared_leaves_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("AAPL", "c"),
                    PrimitiveEventStructure("AAPL", "e")),
        AndCondition(
            GreaterThanCondition(Variable("a", get_opening_price),
                                 Variable("b", get_opening_price)),
            GreaterThanCondition(Variable("b", get_opening_price),
                                 Variable("c", get_opening_price))),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),  # not the same event type  as the pattern 1
                    PrimitiveEventStructure("AMZN", "e")),  # not the same event type  as the pattern 1
        AndCondition(
            GreaterThanCondition(Variable("a", get_opening_price),
                                 Variable("b", get_opening_price)),
            GreaterThanCondition(Variable("b", get_opening_price),
                                 Variable("c", get_opening_price))
        ),
        timedelta(minutes=5)
    )
    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES),
             2, 1)
    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION),
             3, 2)


def leaf_is_root_leaves_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", get_peak_price), 135),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), NegationOperator(PrimitiveEventStructure("AMZN", "b")),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", get_opening_price),
                                 Variable("b", get_opening_price)),
            SmallerThanCondition(Variable("b", get_opening_price),
                                 Variable("c", get_opening_price))),
        timedelta(minutes=5)
    )

    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES),
             0, 1)
    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION),
             0, 2)


def leaf_is_root_leaves_test2():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", get_peak_price), 135),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), NegationOperator(PrimitiveEventStructure("AMZN", "b")),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", get_peak_price), 135),

            GreaterThanCondition(Variable("a", get_opening_price),
                                 Variable("b", get_opening_price)),
            SmallerThanCondition(Variable("b", get_opening_price),
                                 Variable("c", get_opening_price))),
        timedelta(minutes=5)
    )

    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES),
             1, 1)
    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION),
             1, 2)


def three_patterns_no_sharing_leaves_test():
    pattern1 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
            SmallerThanCondition(Variable("b", get_peak_price), Variable("c", get_peak_price))
        ),
        timedelta(minutes=1)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"),
                    PrimitiveEventStructure("DRIV", "b"),
                    PrimitiveEventStructure("MSFT", "c"),
                    PrimitiveEventStructure("DRIV", "d"),
                    PrimitiveEventStructure("MSFT", "e")),
        AndCondition(
            SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
            SmallerThanCondition(Variable("b", get_peak_price), Variable("c", get_peak_price)),
            SmallerThanCondition(Variable("c", get_peak_price), Variable("d", get_peak_price)),
            SmallerThanCondition(Variable("d", get_peak_price), Variable("e", get_peak_price))
        ),
        timedelta(minutes=10)
    )
    pattern3 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", get_opening_price), 7),
            GreaterThanCondition(Variable("b", get_opening_price), 8),
            GreaterThanCondition(Variable("c", get_opening_price), 99)),
        timedelta(minutes=5)
    )

    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2, pattern3],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES),
             0, 1)
    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2, pattern3],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION),
             0, 2)


def three_patterns_partial_sharing_leaves_test():
    pattern1 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
        timedelta(minutes=1)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"),
                    PrimitiveEventStructure("DRIV", "b"),
                    PrimitiveEventStructure("MSFT", "c"),
                    PrimitiveEventStructure("DRIV", "d"),
                    PrimitiveEventStructure("MSFT", "e")),
        AndCondition(
            SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
            SmallerThanCondition(Variable("b", get_peak_price), Variable("c", get_peak_price)),
            SmallerThanCondition(Variable("c", get_peak_price), Variable("d", get_peak_price)),
            SmallerThanCondition(Variable("d", get_peak_price), Variable("e", get_peak_price))
        ),
        timedelta(minutes=10)
    )
    pattern3 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
        timedelta(minutes=5)
    )

    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2, pattern3],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES),
             3, 1)
    myAssert(get_max_size_of_intersection_of_all_patterns([pattern1, pattern2, pattern3],
                                                          approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION),
             5, 2)


if __name__ == '__main__':
    same_leaves_test()
    same_eventsType_different_names_leaves_test()
    same_events_different_condition_leaves_test()
    sameNames_different_eventTypes_leaves_test()
    same_events_different_function_test()
    same_leaves_different_time_stamps_leaves_test()
    distinct_leaves_test()
    partially_shared_leaves_test()
    leaf_is_root_leaves_test()
    leaf_is_root_leaves_test2()
    three_patterns_no_sharing_leaves_test()
    three_patterns_partial_sharing_leaves_test()
