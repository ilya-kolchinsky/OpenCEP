from test.testUtils import *
from datetime import timedelta
from condition.Condition import (
    Variable,
    SimpleCondition,
)
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import (
    GreaterThanCondition,
    SmallerThanCondition,
)
from base.PatternStructure import (
    AndOperator,
    KleeneClosureOperator,
    NegationOperator,
    SeqOperator,
    PrimitiveEventStructure,
)
from base.Pattern import Pattern

nasdaqEventStreamP = FileInputStream(
    os.path.join(absolutePath, "test/EventFiles/NASDAQ_LONG_probabilities.txt")
)


def oneArgumentsearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), 136.27),
        timedelta(minutes=120),
        confidence=0.875,
    )
    runTest("oneProb", [pattern], createTestFile, eventStream=nasdaqEventStreamP)


def oneArgumentsearchTestKleeneClosure(createTestFile=False):
    pattern = Pattern(
        SeqOperator(
            KleeneClosureOperator(
                PrimitiveEventStructure("AAPL", "a"), min_size=1, max_size=5
            )
        ),
        SimpleCondition(
            Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 135
        ),
        timedelta(minutes=5),
        confidence=0.9,
    )
    runTest("oneProbKC", [pattern], createTestFile, eventStream=nasdaqEventStreamP)


def simpleNotTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("AAPL", "a"),
            NegationOperator(PrimitiveEventStructure("AMZN", "b")),
            PrimitiveEventStructure("GOOG", "c"),
        ),
        AndCondition(
            GreaterThanCondition(
                Variable("a", lambda x: x["Opening Price"]),
                Variable("b", lambda x: x["Opening Price"]),
            ),
            SmallerThanCondition(
                Variable("b", lambda x: x["Opening Price"]),
                Variable("c", lambda x: x["Opening Price"]),
            ),
        ),
        timedelta(minutes=5),
        confidence=0.95,
    )

    runTest("simpleNotProb", [pattern], createTestFile, eventStream=nasdaqEventStreamP)


def threePatternsTest(createTestFile=False):
    pattern1 = Pattern(
        AndOperator(
            PrimitiveEventStructure("AAPL", "a"),
            PrimitiveEventStructure("AMZN", "b"),
        ),
        AndCondition(
            SmallerThanCondition(
                Variable("a", lambda x: x["Peak Price"] - x["Opening Price"]),
                Variable("b", lambda x: x["Peak Price"] - x["Opening Price"]),
            ),
        ),
        timedelta(minutes=1),
        confidence=0.96
    )
    pattern2 = Pattern(
        SeqOperator(
            PrimitiveEventStructure("MSFT", "a"),
            PrimitiveEventStructure("DRIV", "b"),
            PrimitiveEventStructure("MSFT", "c"),
            PrimitiveEventStructure("DRIV", "d"),
            PrimitiveEventStructure("MSFT", "e"),
        ),
        AndCondition(
            SmallerThanCondition(
                Variable("a", lambda x: x["Peak Price"]),
                Variable("b", lambda x: x["Peak Price"]),
            ),
            SmallerThanCondition(
                Variable("b", lambda x: x["Peak Price"]),
                Variable("c", lambda x: x["Peak Price"]),
            ),
            SmallerThanCondition(
                Variable("c", lambda x: x["Peak Price"]),
                Variable("d", lambda x: x["Peak Price"]),
            ),
            SmallerThanCondition(
                Variable("d", lambda x: x["Peak Price"]),
                Variable("e", lambda x: x["Peak Price"]),
            ),
        ),
        timedelta(minutes=10),
        confidence=0.6,
    )
    pattern3 = Pattern(
        SeqOperator(
            PrimitiveEventStructure("AAPL", "a"),
            PrimitiveEventStructure("AMZN", "b"),
            PrimitiveEventStructure("GOOG", "c"),
        ),
        AndCondition(
            GreaterThanCondition(
                Variable("a", lambda x: x["Opening Price"]),
                Variable("c", lambda x: x["Opening Price"]),
            ),
            GreaterThanCondition(
                Variable("a", lambda x: x["Opening Price"]),
                Variable("b", lambda x: x["Opening Price"]),
            ),
        ),
        timedelta(minutes=5),
        confidence=0.6,
    )

    runMultiTest("ThreePatternTestProb", [pattern1, pattern2, pattern3], createTestFile, eventStream=nasdaqEventStreamP)
