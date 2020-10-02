from test.testUtils import *
from datetime import timedelta
from base.Formula import GreaterThanFormula, SmallerThanFormula, IdentifierTerm, AtomicTerm, AndFormula
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure, NegationOperator
from base.Pattern import Pattern

#New tests for multipattern first approach.
#NASDAQ SHORT


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
    runTest("FirstMultiPattern", [pattern1, pattern2], createTestFile)

