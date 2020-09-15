from test.testUtils import *
from datetime import timedelta
from base.Formula import GreaterThanFormula, SmallerThanFormula, IdentifierTerm, AtomicTerm, AndFormula
from base.PatternStructure import AndOperator, SeqOperator, QItem, KleeneClosureOperator
from base.Pattern import Pattern

from parallerization.ParallelTreeWorkloadFramework import ParallelTreeWorkloadFramework

def onlyDataSplit_oneArgumentsearchTest(createTestFile=False):
        pattern = Pattern(
            SeqOperator([QItem("AAPL", "a")]),
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), AtomicTerm(135)),
            timedelta(minutes=120)
        )
        workload = ParallelTreeWorkloadFramework(2, True, pattern_size=1, pattern=pattern)
        runTest("one", [pattern], createTestFile, workloadfr=workload)
