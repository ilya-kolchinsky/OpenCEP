from datetime import timedelta

from base.PatternStructure import SeqOperator, PrimitiveEventStructure, NegationOperator, AndOperator
from condition.BaseRelationCondition import GreaterThanCondition, SmallerThanCondition, SmallerThanEqCondition, \
    GreaterThanEqCondition
from condition.CompositeCondition import AndCondition
from condition.Condition import Variable
from test.testUtils import *


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

    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=4, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=7, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=7, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


def same_events_type_different_names_test():
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
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=3, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=3, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=5, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


def same_events_different_condition_test():
    pattern1 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a"),
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
        AndOperator(PrimitiveEventStructure("AAPL", "a"),
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

    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=3, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=4, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=5, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


def sameNames_different_event_types():
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
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=3, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=5, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=5, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


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
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=3, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=3, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=5, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


def same_leaves_different_time_stamps_test():
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

    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=3, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=5, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=5, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


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
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=0, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=0, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=0, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


def partially_shared_test():
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
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=2, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=3, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=3, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


def leaf_is_root_test():
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

    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=0, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=0, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=0, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


def leaf_is_root_test_2():
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

    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=1, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=1, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION)
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=1, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


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

    run_tree_plan_union_test(patterns=[pattern1, pattern2, pattern3], expected=0,
                             approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES)
    run_tree_plan_union_test(patterns=[pattern1, pattern2, pattern3], expected=0,
                             approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION)
    run_tree_plan_union_test(patterns=[pattern1, pattern2, pattern3], expected=0,
                             approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


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

    run_tree_plan_union_test(patterns=[pattern1, pattern2, pattern3], expected=3,
                             approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES)
    run_tree_plan_union_test(patterns=[pattern1, pattern2, pattern3], expected=5,
                             approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION)
    run_tree_plan_union_test(patterns=[pattern1, pattern2, pattern3], expected=5,
                             approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


def three_patterns_ordered_events_test_1():
    pattern1 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
        timedelta(minutes=1)
    )
    pattern2 = Pattern(
        AndOperator(
            PrimitiveEventStructure("MSFT", "e"),
            PrimitiveEventStructure("AAPL", "a"),
            PrimitiveEventStructure("AMZN", "b"),
            PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
        ),
        timedelta(minutes=10)
    )
    run_tree_plan_union_test(patterns=[pattern1, pattern2], expected=5, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


def three_patterns_ordered_events_test_2():
    pattern1 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
        timedelta(minutes=1)
    )
    pattern2 = Pattern(
        AndOperator(
            PrimitiveEventStructure("MSFT", "e"),
            PrimitiveEventStructure("AAPL", "a"),
            PrimitiveEventStructure("AMZN", "b"),
            PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
        ),
        timedelta(minutes=10)
    )
    pattern3 = Pattern(
        AndOperator(PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("MSFT", "e")),
        SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
        timedelta(minutes=5)
    )

    run_tree_plan_union_test(patterns=[pattern1, pattern2, pattern3], expected=10,
                             approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


def three_patterns_ordered_events_test_3():
    pattern1 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "b"),
                    PrimitiveEventStructure("AMZN", "a"),
                    PrimitiveEventStructure("GOOG", "c")),
        SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
        timedelta(minutes=1)
    )
    pattern2 = Pattern(
        AndOperator(
            PrimitiveEventStructure("MSFT", "e"),
            PrimitiveEventStructure("AAPL", "a"),
            PrimitiveEventStructure("AMZN", "b"),
            PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
        ),
        timedelta(minutes=10)
    )
    pattern3 = Pattern(
        AndOperator(PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("MSFT", "e")),
        SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
        timedelta(minutes=5)
    )

    run_tree_plan_union_test(patterns=[pattern1, pattern2, pattern3], expected=6,
                             approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


def three_patterns_ordered_events_test_4():
    patterns = [
        Pattern(
            AndOperator(PrimitiveEventStructure("AAPL", "a"),
                        PrimitiveEventStructure("AMZN", "b"),
                        PrimitiveEventStructure("GOOG", "c")),
            AndCondition(
                SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
                SmallerThanCondition(Variable("c", get_peak_price), 135)
            ),
            timedelta(minutes=3)
        ),
        Pattern(
            AndOperator(
                PrimitiveEventStructure("GOOG", "d"),
                PrimitiveEventStructure("AAPL", "a"),
                PrimitiveEventStructure("AMZN", "b")),
            AndCondition(
                SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
                SmallerThanCondition(Variable("d", get_peak_price), 135)
            ),
            timedelta(minutes=3)
        ),
        Pattern(
            AndOperator(PrimitiveEventStructure("AAPL", "e"),
                        PrimitiveEventStructure("AMZN", "b"),
                        PrimitiveEventStructure("AAPL", "a")),
            SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
            timedelta(minutes=3)
        )
    ]

    run_tree_plan_union_test(patterns=patterns, expected=6, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


def three_patterns_ordered_events_test_5():
    pattern1 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        SmallerThanCondition(Variable("a", get_peak_price), Variable("c", get_peak_price)),
        timedelta(minutes=1)
    )
    pattern2 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
        ),
        timedelta(minutes=10)
    )
    pattern3 = Pattern(
        AndOperator(PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("MSFT", "e")),
        SmallerThanCondition(Variable("a", get_peak_price), Variable("e", get_peak_price)),
        timedelta(minutes=5)
    )
    run_tree_plan_union_test(patterns=[pattern1, pattern2, pattern3], expected=7,
                             approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


def four_patterns_ordered_events_test_1():
    patterns = [
        Pattern(
            AndOperator(PrimitiveEventStructure("AAPL", "a"),
                        PrimitiveEventStructure("AMZN", "b"),
                        PrimitiveEventStructure("GOOG", "c")),
            AndCondition(
                SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
                SmallerThanCondition(Variable("c", get_peak_price), 135)
            ),
            timedelta(minutes=3)
        ),
        Pattern(
            AndOperator(
                PrimitiveEventStructure("GOOG", "d"),
                PrimitiveEventStructure("AAPL", "a"),
                PrimitiveEventStructure("AMZN", "b")),
            AndCondition(
                SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
                SmallerThanCondition(Variable("d", get_peak_price), 135)
            ),
            timedelta(minutes=3)
        ),
        Pattern(
            AndOperator(PrimitiveEventStructure("AAPL", "e"),
                        PrimitiveEventStructure("AMZN", "b"),
                        PrimitiveEventStructure("AAPL", "a")),
            SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
            timedelta(minutes=3)
        ),
        Pattern(
            AndOperator(PrimitiveEventStructure("AAPL", "e"),
                        PrimitiveEventStructure("AMZN", "b"),
                        PrimitiveEventStructure("AAPL", "a")),
            SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
            timedelta(minutes=3)
        ),
    ]

    run_tree_plan_union_test(patterns=patterns, expected=11, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


def four_patterns_ordered_events_test_2():
    pattern1 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
        timedelta(minutes=1)
    )
    pattern2 = Pattern(
        AndOperator(
            PrimitiveEventStructure("MSFT", "e"),
            PrimitiveEventStructure("AAPL", "a"),
            PrimitiveEventStructure("AMZN", "b"),
            PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
        ),
        timedelta(minutes=10)
    )
    pattern3 = Pattern(
        AndOperator(PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("MSFT", "e")),
        SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
        timedelta(minutes=5)
    )
    pattern4 = Pattern(
        AndOperator(PrimitiveEventStructure("MSFT", "e"),
                    PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b")),
        SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
        timedelta(minutes=5)
    )
    run_tree_plan_union_test(patterns=[pattern1, pattern2, pattern3, pattern4], expected=15,
                             approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


def equal_patterns_test():
    def rotate(lst: list, n: int) -> list:
        return lst[-n:] + lst[:-n]

    patters_number = 8
    events = [PrimitiveEventStructure("AAPL", "a"),
              PrimitiveEventStructure("AMZN", "b"),
              PrimitiveEventStructure("GOOG", "c")]
    patterns = [
        Pattern(
            AndOperator(*rotate(events, i % patters_number)),
            AndCondition(
                SmallerThanCondition(Variable("a", get_peak_price), Variable("b", get_peak_price)),
                SmallerThanCondition(Variable("c", get_peak_price), 135)
            ),
            timedelta(minutes=3)
        )
        for i in range(patters_number)]

    run_tree_plan_union_test(patterns=patterns, expected=7 * 3,
                             approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES)
    run_tree_plan_union_test(patterns=patterns, expected=31,
                             approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION)
    run_tree_plan_union_test(patterns=patterns, expected=(patters_number - 1) * 5,
                             approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


def negation_operators_test():
    patterns = [
        Pattern(
            SeqOperator(NegationOperator(PrimitiveEventStructure("TYP1", "x")),
                        PrimitiveEventStructure("AAPL", "a"),
                        PrimitiveEventStructure("AMZN", "b")
                        ),
            AndCondition(
                GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                     Variable("b", lambda x: x["Opening Price"])),
            ),
            timedelta(minutes=5)
        ),

        Pattern(
            SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                        PrimitiveEventStructure("AMZN", "b"),
                        PrimitiveEventStructure("GOOG", "c")),
            AndCondition(
                GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                     Variable("b", lambda x: x["Opening Price"])),
                GreaterThanCondition(Variable("c", lambda x: x["Opening Price"]),
                                     Variable("b", lambda x: x["Opening Price"]))
            ),
            timedelta(minutes=5)
        )
    ]

    run_tree_plan_union_test(patterns=patterns, expected=3, approach=MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION)


if __name__ == '__main__':
    print_result("TEST", "UNION APPROACH", "RESULT")
    print("=" * 100)
    same_leaves_test()
    same_events_type_different_names_test()
    same_events_different_condition_test()
    sameNames_different_event_types()
    same_events_different_function_test()
    same_leaves_different_time_stamps_test()
    distinct_leaves_test()
    partially_shared_test()
    leaf_is_root_test()
    leaf_is_root_test_2()
    three_patterns_no_sharing_leaves_test()
    three_patterns_partial_sharing_leaves_test()
    three_patterns_ordered_events_test_1()
    three_patterns_ordered_events_test_2()
    three_patterns_ordered_events_test_3()
    three_patterns_ordered_events_test_4()
    three_patterns_ordered_events_test_5()
    four_patterns_ordered_events_test_1()
    four_patterns_ordered_events_test_2()
    equal_patterns_test()

    # negation operators
    negation_operators_test()
