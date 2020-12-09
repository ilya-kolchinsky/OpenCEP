from test.testUtils import *
from datetime import timedelta
from condition.Condition import Variable, TrueCondition, BinaryCondition, SimpleCondition
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import EqCondition, GreaterThanCondition, GreaterThanEqCondition, SmallerThanEqCondition
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure
from base.Pattern import Pattern

nasdaqEventStreamP = FileInputStream(os.path.join(absolutePath, "test/EventFiles/NASDAQ_LONG_probabilities.txt"))

def oneArgumentsearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), 136.27),
        timedelta(minutes=120),
        confidence=0.875
    )
    runTest("oneProb", [pattern], createTestFile, eventStream=nasdaqEventStreamP)