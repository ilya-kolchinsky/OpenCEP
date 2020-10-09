from test.testUtils import *
from datetime import timedelta
from base.Formula import GreaterThanEqFormula, SmallerThanEqFormula, GreaterThanFormula, SmallerThanFormula, IdentifierTerm, AtomicTerm, AndFormula
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure, NegationOperator
from base.Pattern import Pattern

currentPath = pathlib.Path(os.path.dirname(__file__))
absolutePath = str(currentPath.parent)
sys.path.append(absolutePath)



def twoPatternsOneArgument(createTestFile = False):
    pattern1 = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a")]),
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), AtomicTerm(135)),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), NegationOperator(PrimitiveEventStructure("AMZN", "b")), PrimitiveEventStructure("GOOG", "c")]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )

    runMultiTest("FirstMultiPattern", [pattern1, pattern2], createTestFile)

def bigMultiPatternTest(createTestFile = False):
    pattern1 = Pattern(
        SeqOperator([PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b"), PrimitiveEventStructure("GOOG", "c")]),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                               IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                               IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    pattern2 = Pattern(
        SeqOperator([PrimitiveEventStructure("AMZN", "x1"), PrimitiveEventStructure("AMZN", "x2"), PrimitiveEventStructure("AMZN", "x3")]),
        AndFormula(
            SmallerThanEqFormula(IdentifierTerm("x1", lambda x: x["Lowest Price"]), AtomicTerm(75)),
            AndFormula(
                GreaterThanEqFormula(IdentifierTerm("x2", lambda x: x["Peak Price"]), AtomicTerm(78)),
                SmallerThanEqFormula(IdentifierTerm("x3", lambda x: x["Lowest Price"]),
                                     IdentifierTerm("x1", lambda x: x["Lowest Price"]))
            )
        ),
        timedelta(days=1)
    )

    runMultiTest("BigMultiPattern", [pattern1, pattern2], createTestFile)

def threePatternTest(createTestFile = False):
    pattern1 = Pattern(
        AndOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                     PrimitiveEventStructure("GOOG", "c")]),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                               IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                               IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=1)
    )
    pattern2 = Pattern(
        SeqOperator([PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"), PrimitiveEventStructure("MSFT", "c"), PrimitiveEventStructure("DRIV", "d"), PrimitiveEventStructure("MSFT", "e")]),
        AndFormula(
            AndFormula(
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                                   IdentifierTerm("b", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                                   IdentifierTerm("c", lambda x: x["Peak Price"]))
            ),
            AndFormula(
                SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                                   IdentifierTerm("d", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("d", lambda x: x["Peak Price"]),
                                   IdentifierTerm("e", lambda x: x["Peak Price"]))
            )
        ),
        timedelta(minutes=10)
    )
    pattern3 = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"])),
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )

    runMultiTest("ThreePatternTest", [pattern1, pattern2, pattern3], createTestFile)

def rootAndInner(createTestFile = False):
    pattern1 = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a")]),
        GreaterThanEqFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), AtomicTerm(135)),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")]),
        AndFormula(
            GreaterThanEqFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), AtomicTerm(135)),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                               IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )

    runMultiTest("RootAndInner", [pattern1, pattern2], createTestFile)

def differentTimeStamps(createTestFile = False):
    pattern1 = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")]),
        AndFormula(
            GreaterThanEqFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), AtomicTerm(135)),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                               IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")]),
        AndFormula(
            GreaterThanEqFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), AtomicTerm(135)),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                               IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=2)
    )

    runMultiTest("DifferentTimeStamp", [pattern1, pattern2], createTestFile)

def multiPatternShare(createTestFile = False):
    pattern1 = Pattern(
        SeqOperator([PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"), PrimitiveEventStructure("MSFT", "c"), PrimitiveEventStructure("DRIV", "d"), PrimitiveEventStructure("MSFT", "e")]),
        AndFormula(
            AndFormula(
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                                   IdentifierTerm("b", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                                   IdentifierTerm("c", lambda x: x["Peak Price"]))
            ),
            AndFormula(
                SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                                   IdentifierTerm("d", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("d", lambda x: x["Peak Price"]),
                                   IdentifierTerm("e", lambda x: x["Peak Price"]))
            )
        ),
        timedelta(minutes=10)
    )
    pattern2 = Pattern(
        SeqOperator([PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"), PrimitiveEventStructure("MSFT", "c"), PrimitiveEventStructure("DRIV", "d"), PrimitiveEventStructure("MSFT", "e")]),
        AndFormula(
            AndFormula(
                GreaterThanEqFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), AtomicTerm(135)),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                                   IdentifierTerm("a", lambda x: x["Peak Price"]))
            ),
            AndFormula(
                SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                                   IdentifierTerm("b", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("d", lambda x: x["Peak Price"]),
                                   IdentifierTerm("e", lambda x: x["Peak Price"]))
            )
        ),
        timedelta(minutes=10)
    )
    pattern3 = Pattern(
        SeqOperator([PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"), PrimitiveEventStructure("MSFT", "c"), PrimitiveEventStructure("DRIV", "d"), PrimitiveEventStructure("MSFT", "e")]),
        AndFormula(
            AndFormula(
                GreaterThanEqFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), AtomicTerm(135)),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                                   IdentifierTerm("d", lambda x: x["Peak Price"]))
            ),
            AndFormula(
                SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                                   IdentifierTerm("d", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("d", lambda x: x["Peak Price"]),
                                   IdentifierTerm("e", lambda x: x["Peak Price"]))
            )
        ),
        timedelta(minutes=10)
    )


    runMultiTest("multiPatternShare", [pattern1, pattern2, pattern3], createTestFile)

#SHARING EQUIVALENT SUBTREES
def onePatternIncludesOther(createTestFile = False):
    pattern1 = Pattern(
        SeqOperator([PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b"),
                     PrimitiveEventStructure("AAPL", "c")]),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                               IdentifierTerm("b", lambda x: x["Peak Price"])),
            GreaterThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                               IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )

    pattern2 = Pattern(
        SeqOperator([PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b")]),
        SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                           IdentifierTerm("b", lambda x: x["Peak Price"]))
        ,
        timedelta(minutes=3)
    )

    eval_mechanism_params = TreeBasedEvaluationMechanismParameters(TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                                                     TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL),
                                           TreeStorageParameters(sort_storage=False,
                                                                 clean_up_interval=10,
                                                                 prioritize_sorting_by_timestamp=True),
                                                                  MultiPatternEvaluationApproach.SUBTREES_UNION)
    runMultiTest("onePatternIncludesOther", [pattern1, pattern2], createTestFile, eval_mechanism_params)

