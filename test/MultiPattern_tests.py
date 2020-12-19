from test.testUtils import *
from datetime import timedelta
from condition.Condition import Variable
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import GreaterThanCondition, SmallerThanCondition, GreaterThanEqCondition, \
    SmallerThanEqCondition
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure, NegationOperator
from base.Pattern import Pattern
from plan.multi.MultiPatternEvaluationParameters import *

currentPath = pathlib.Path(os.path.dirname(__file__))
absolutePath = str(currentPath.parent)
sys.path.append(absolutePath)

get_opening_price = lambda x: x["Opening Price"]
get_peak_price = lambda x: x["Opening Price"]

"""
Simple multi-pattern test with 2 patterns
"""


def leafIsRoot(createTestFile=False):
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

    runMultiTest("FirstMultiPattern", [pattern1, pattern2], createTestFile)


"""
multi-pattern test 2 completely distinct patterns
"""


def distinctPatterns(createTestFile=False):
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", get_peak_price),
                                 Variable("b", get_peak_price)),
            SmallerThanCondition(Variable("b", get_peak_price),
                                 Variable("c", get_peak_price))
        ),
        timedelta(minutes=3)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "x1"), PrimitiveEventStructure("AMZN", "x2"), PrimitiveEventStructure("AMZN", "x3")),
        AndCondition(
            SmallerThanEqCondition(Variable("x1", lambda x: x["Lowest Price"]), 75),
            GreaterThanEqCondition(Variable("x2", get_peak_price), 78),
            SmallerThanEqCondition(Variable("x3", lambda x: x["Lowest Price"]),
                                   Variable("x1", lambda x: x["Lowest Price"]))
        ),
        timedelta(days=1)
    )

    runMultiTest("BigMultiPattern", [pattern1, pattern2], createTestFile)


"""
multi-pattern test with 3 patterns and leaf sharing
"""


def threePatternsTest(createTestFile=False):
    pattern1 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", get_peak_price),
                                 Variable("b", get_peak_price)),
            SmallerThanCondition(Variable("b", get_peak_price),
                                 Variable("c", get_peak_price))
        ),
        timedelta(minutes=1)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"),
                    PrimitiveEventStructure("MSFT", "c"), PrimitiveEventStructure("DRIV", "d"),
                    PrimitiveEventStructure("MSFT", "e")),
        AndCondition(
            SmallerThanCondition(Variable("a", get_peak_price),
                                 Variable("b", get_peak_price)),
            SmallerThanCondition(Variable("b", get_peak_price),
                                 Variable("c", get_peak_price)),
            SmallerThanCondition(Variable("c", get_peak_price),
                                 Variable("d", get_peak_price)),
            SmallerThanCondition(Variable("d", get_peak_price),
                                 Variable("e", get_peak_price))
        ),
        timedelta(minutes=10)
    )
    pattern3 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", get_opening_price), Variable("c", get_opening_price)),
            GreaterThanCondition(Variable("a", get_opening_price), Variable("b", get_opening_price))),
        timedelta(minutes=5)
    )

    runMultiTest("ThreePatternTest", [pattern1, pattern2, pattern3], createTestFile)


"""
multi-pattern test checking case where output node is not a root
"""


def rootAndInner(createTestFile=False):
    # similar to leafIsRoot, but the time windows are different
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanEqCondition(Variable("a", get_peak_price), 135),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanEqCondition(Variable("a", get_peak_price), 135),
            SmallerThanCondition(Variable("b", get_peak_price),
                                 Variable("c", get_peak_price))
        ),
        timedelta(minutes=3)
    )

    runMultiTest("RootAndInner", [pattern1, pattern2], createTestFile)


"""
multi-pattern test 2 identical patterns with different time stamp
"""


def samePatternDifferentTimeStamps(createTestFile=False):
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanEqCondition(Variable("a", get_peak_price), 135),
            SmallerThanCondition(Variable("b", get_peak_price),
                                 Variable("c", get_peak_price))
        ),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanEqCondition(Variable("a", get_peak_price), 135),
            SmallerThanCondition(Variable("b", get_peak_price),
                                 Variable("c", get_peak_price))
        ),
        timedelta(minutes=2)
    )

    runMultiTest("DifferentTimeStamp", [pattern1, pattern2], createTestFile)


"""
multi-pattern test sharing equivalent subtrees
"""


def onePatternIncludesOther(createTestFile=False):
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b"),
                    PrimitiveEventStructure("AAPL", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", get_peak_price),
                                 Variable("b", get_peak_price)),
            GreaterThanCondition(Variable("b", get_peak_price),
                                 Variable("c", get_peak_price))
        ),
        timedelta(minutes=3)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b")),
        SmallerThanCondition(Variable("a", get_peak_price),
                             Variable("b", get_peak_price))
        ,
        timedelta(minutes=3)
    )

    eval_mechanism_params = TreeBasedEvaluationMechanismParameters(TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                                                                             TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL),
                                                                   TreeStorageParameters(sort_storage=False,
                                                                                         clean_up_interval=10,
                                                                                         prioritize_sorting_by_timestamp=True),
                                                                   MultiPatternEvaluationParameters(
                                                                       MultiPatternEvaluationApproaches.TRIVIAL_SHARING_LEAVES))
    runMultiTest("onePatternIncludesOther", [pattern1, pattern2], createTestFile, eval_mechanism_params)


"""
multi-pattern test sharing equivalent subtrees but condition is not equal
"""


def onePatternIncludesOtherNewApproach(createTestFile=False):
    getattr_f = lambda x: x["Peak Price"]
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b"),
                    PrimitiveEventStructure("AAPL", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", get_peak_price),
                                 Variable("b", get_peak_price)),
            GreaterThanCondition(Variable("b", get_peak_price),
                                 Variable("c", get_peak_price))
        ),
        timedelta(minutes=3)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b"),
                    PrimitiveEventStructure("AAPL", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", get_peak_price),
                                 Variable("b", get_peak_price)),
            GreaterThanCondition(Variable("b", get_peak_price),
                                 Variable("c", get_peak_price))
        ),
        timedelta(minutes=3)
    )

    eval_mechanism_params = TreeBasedEvaluationMechanismParameters(TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                                                                             TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL),
                                                                   TreeStorageParameters(sort_storage=False,
                                                                                         clean_up_interval=10,
                                                                                         prioritize_sorting_by_timestamp=True),
                                                                   MultiPatternEvaluationParameters(
                                                                       MultiPatternEvaluationApproaches.TREE_PLAN_UNIFIED_TREE))
    runMultiTest("onePatternIncludesOther", [pattern1, pattern2], createTestFile, eval_mechanism_params)


"""
multi-pattern test multiple patterns share the same output node
"""


def samePatternSharingRoot(createTestFile=False):
    hierarchyPattern = Pattern(
        AndOperator(PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("AAPL", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                 Variable("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=1)
    )

    hierarchyPattern2 = Pattern(
        AndOperator(PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("AAPL", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                 Variable("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=0.5)
    )

    hierarchyPattern3 = Pattern(
        AndOperator(PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("AAPL", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                 Variable("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=0.1)
    )

    eval_mechanism_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL),
        TreeStorageParameters(sort_storage=False,
                              clean_up_interval=10,
                              prioritize_sorting_by_timestamp=True),
        MultiPatternEvaluationParameters(MultiPatternEvaluationApproaches.SUBTREES_UNION))

    runMultiTest('hierarchyMultiPattern', [hierarchyPattern, hierarchyPattern2, hierarchyPattern3], createTestFile, eval_mechanism_params)


"""
multi-pattern test several patterns sharing the same subtree
"""


def severalPatternShareSubtree(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"), NegationOperator(PrimitiveEventStructure("TYP1", "x")),
                    NegationOperator(PrimitiveEventStructure("TYP2", "y")),
                    NegationOperator(PrimitiveEventStructure("TYP3", "z"))),
        AndCondition(
            GreaterThanCondition(Variable("a", get_opening_price),
                                 Variable("b", get_opening_price)),
            SmallerThanCondition(Variable("b", get_opening_price),
                                 Variable("c", get_opening_price))),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                                   PrimitiveEventStructure("TYP1", "x")),
                       GreaterThanCondition(Variable("a", get_opening_price),
                                            Variable("b", get_opening_price)),
                       timedelta(minutes=5)
                       )

    pattern3 = Pattern(SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
                       GreaterThanCondition(Variable("a", get_opening_price),
                                            Variable("b", get_opening_price)),
                       timedelta(minutes=5)
                       )

    eval_mechanism_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL),
        TreeStorageParameters(sort_storage=False,
                              clean_up_interval=10,
                              prioritize_sorting_by_timestamp=True),
        MultiPatternEvaluationParameters(MultiPatternEvaluationApproaches.SUBTREES_UNION))

    runMultiTest("threeSharingSubtrees", [pattern, pattern2, pattern3], createTestFile, eval_mechanism_params)


"""
multi-pattern test patterns sharing inner nodes and not leaves
"""


def notInTheBeginningShare(createTestFile=False):
    pattern1 = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("TYP1", "x")),
                    NegationOperator(PrimitiveEventStructure("TYP2", "y")),
                    NegationOperator(PrimitiveEventStructure("TYP3", "z")), PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", get_opening_price),
                                 Variable("b", get_opening_price)),
            SmallerThanCondition(Variable("b", get_opening_price),
                                 Variable("c", get_opening_price))),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("TYP1", "x")),
                    NegationOperator(PrimitiveEventStructure("TYP2", "y")),
                    PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b")),
        GreaterThanCondition(Variable("a", get_opening_price),
                             Variable("b", get_opening_price)),
        timedelta(minutes=5)
    )

    pattern3 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", get_opening_price),
                                 Variable("b", get_opening_price)),
            GreaterThanCondition(Variable("c", get_opening_price),
                                 Variable("b", get_opening_price))
        ),
        timedelta(minutes=5)
    )

    eval_mechanism_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL),
        TreeStorageParameters(sort_storage=False,
                              clean_up_interval=10,
                              prioritize_sorting_by_timestamp=True),
        MultiPatternEvaluationParameters(MultiPatternEvaluationApproaches.SUBTREES_UNION))

    runMultiTest("MultipleNotBeginningShare", [pattern1, pattern2, pattern3], createTestFile, eval_mechanism_params)


"""
multi-pattern test sharing internal node between patterns
"""


def multipleParentsForInternalNode(createTestFile=False):
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("c", lambda x: x["Peak Price"]), 500)
        ),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("c", lambda x: x["Peak Price"]), 530)
        ),
        timedelta(minutes=3)
    )

    pattern3 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("FB", "e")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("e", lambda x: x["Peak Price"]), 520)
        ),
        timedelta(minutes=5)
    )

    pattern4 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("LI", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("c", lambda x: x["Peak Price"]), 100)
        ),
        timedelta(minutes=2)
    )

    eval_mechanism_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL),
        TreeStorageParameters(sort_storage=False,
                              clean_up_interval=10,
                              prioritize_sorting_by_timestamp=True),
        MultiPatternEvaluationParameters(MultiPatternEvaluationApproaches.SUBTREES_UNION))

    runMultiTest("multipleParentsForInternalNode", [pattern1, pattern2, pattern3, pattern4], createTestFile, eval_mechanism_params)
