from test.testUtils import *
from datetime import timedelta
from base.Formula import GreaterThanFormula, IdentifierTerm, AtomicTerm, AndFormula
from base.PatternStructure import SeqOperator, PrimitiveEventStructure
from base.Pattern import Pattern

from parallerization.tree_implemintation.ParallelTreeWorkloadFramework import ParallelTreeWorkloadFramework


def onlyDataSplit_oneArgumentsearchTest(createTestFile=False):
        pattern = Pattern(
            SeqOperator([PrimitiveEventStructure("AAPL", "a")]),
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), AtomicTerm(135)),
            timedelta(minutes=120)
        )
        workload = ParallelTreeWorkloadFramework(pattern, execution_units=3, is_data_parallelized=True,
                                                 is_structure_parallelized=False, num_of_families=0)
        runTest("one", [pattern], createTestFile, work_load_fr=workload)


def onlyTreeSplit_oneArgumentsearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a")]),
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), AtomicTerm(135)),
        timedelta(minutes=120)
    )
    workload = ParallelTreeWorkloadFramework(pattern, execution_units=2, is_data_parallelized=True,
                                                 is_structure_parallelized=False, num_of_families=0)
    runTest("one", [pattern], createTestFile, work_load_fr=workload)

def onlyTreeSplitsimplePatternSearchTest(createTestFile=False):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > b.OpeningPrice
        AND b.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AVID", "c")]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("b", lambda x: x["Opening Price"])),
            GreaterThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    workload_framework = ParallelTreeWorkloadFramework(pattern, execution_units=1, is_data_parallelized=False,
                                                 is_structure_parallelized=True, num_of_families=0)
    runTest("simple", [pattern], createTestFile, work_load_fr=workload_framework)

