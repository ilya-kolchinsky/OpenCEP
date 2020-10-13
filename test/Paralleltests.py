from test.testUtils import *
from datetime import timedelta
from base.Formula import GreaterThanFormula, IdentifierTerm, AtomicTerm, AndFormula, SmallerThanFormula
from base.PatternStructure import SeqOperator, PrimitiveEventStructure, NegationOperator
from base.Pattern import Pattern

from parallelism.tree_implemintation.ParallelTreeWorkloadFrameworkImplementation import ParallelTreeWorkloadFrameworkImplementation
from parallelism.tree_implemintation.WorkloadFrameworkForSingleEvent import WorkloadFrameworkImplementationForSingleEventTests


def MultiStructureMultiDataOneFamily(createTestFile=False):
        pattern = Pattern(
            SeqOperator([PrimitiveEventStructure("AAPL", "a")]),
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), AtomicTerm(135)),
            timedelta(minutes=120)
        )
        workload = ParallelTreeWorkloadFrameworkImplementation(pattern, execution_units=1, is_data_parallel=True,
                                                               is_structure_parallel=True, num_of_families=1)
        print("Running test for multiple structures, multiple data parts with 1 family, 1 structure, 1 data part")

        runTest("one", [pattern], createTestFile, work_load_fr=workload)


def MultiStructureMultiDataTwoFamily(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a")]),
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), AtomicTerm(135)),
        timedelta(minutes=120)
    )
    workload = ParallelTreeWorkloadFrameworkImplementation(pattern, execution_units=1, is_data_parallel=True,
                                                           is_structure_parallel=True, num_of_families=2)
    print("Running test for multiple structures, multiple data parts with 2 family, 1 structure, 2 families")

    runTest("one", [pattern], createTestFile, work_load_fr=workload)


def MultiStructureMultiDataTwoFamily2(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                     PrimitiveEventStructure("AVID", "c")]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            GreaterThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    workload_framework = ParallelTreeWorkloadFrameworkImplementation(pattern, execution_units=3, is_data_parallel=True,
                                                                     is_structure_parallel=True, num_of_families=1)

    print("Running test for multiple structures, multiple data part with 3 structures, 1 data part, 1 family")

    runTest("simple", [pattern], createTestFile, work_load_fr=workload_framework)


def SingleStructureMultiData1(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a")]),
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), AtomicTerm(135)),
        timedelta(minutes=120)
    )
    workload = ParallelTreeWorkloadFrameworkImplementation(pattern, execution_units=1, is_data_parallel=True,
                                                           is_structure_parallel=False, num_of_families=0)
    print("Running test for single structure, multiple data parts with 1 structure, 1 data part")

    runTest("one", [pattern], createTestFile, work_load_fr=workload)

def SingleStructureMultiData4(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a")]),
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), AtomicTerm(135)),
        timedelta(minutes=120)
    )
    workload = WorkloadFrameworkImplementationForSingleEventTests(pattern, execution_units=1, is_data_parallel=True,
                                                                  is_structure_parallel=False, num_of_families=0)
    print("Running test for single structure, multiple data parts with 1 structure, 1 data part")

    runTest("one", [pattern], createTestFile, work_load_fr=workload)

def SingleStructureMultiData2(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a")]),
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), AtomicTerm(135)),
        timedelta(minutes=120)
    )
    workload = WorkloadFrameworkImplementationForSingleEventTests(pattern, execution_units=3, is_data_parallel=True,
                                                                  is_structure_parallel=False, num_of_families=0)

    print("Running test for single structure, multiple data parts with 1 structure, 3 data part")

    runTest("one", [pattern], createTestFile, work_load_fr=workload)

def SingleStructureMultiData3(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a")]),
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), AtomicTerm(135)),
        timedelta(minutes=120)
    )
    workload = WorkloadFrameworkImplementationForSingleEventTests(pattern, execution_units=5, is_data_parallel=True,
                                                                  is_structure_parallel=False, num_of_families=0)

    print("Running test for single structure, multiple data parts with 1 structure, 5 data parts")

    runTest("one", [pattern], createTestFile, work_load_fr=workload)

def MultipleStructuresSingleData1(createTestFile=False):
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
    workload_framework = ParallelTreeWorkloadFrameworkImplementation(pattern, execution_units=1, is_data_parallel=False,
                                                                     is_structure_parallel=True, num_of_families=0)

    print("Running test for multiple structures, single data part with 1 structure, 1 data part")

    runTest("simple", [pattern], createTestFile, work_load_fr=workload_framework)


def MultipleStructuresSingleData2(createTestFile=False):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > b.OpeningPrice
        AND b.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                     PrimitiveEventStructure("AVID", "c")]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            GreaterThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    workload_framework = ParallelTreeWorkloadFrameworkImplementation(pattern, execution_units=3, is_data_parallel=False,
                                                                     is_structure_parallel=True, num_of_families=0)

    print("Running test for multiple structures, single data part with 3 structures, 1 data part")

    runTest("simple", [pattern], createTestFile, work_load_fr=workload_framework)


def MultipleStructuresSingleData3(createTestFile=False):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > b.OpeningPrice
        AND b.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                     PrimitiveEventStructure("AVID", "c")]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            GreaterThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    workload_framework = ParallelTreeWorkloadFrameworkImplementation(pattern, execution_units=5, is_data_parallel=False,
                                                                     is_structure_parallel=True, num_of_families=0)

    print("Running test for multiple structures, single data part with 3 structures, 1 data part")

    runTest("simple", [pattern], createTestFile, work_load_fr=workload_framework)


def MultipleStructuresSingleData4(createTestFile=False):
    """
    This pattern is looking for a race between driv and microsoft in ten minutes
    PATTERN SEQ(MicrosoftStockPriceUpdate a, DrivStockPriceUpdate b, MicrosoftStockPriceUpdate c, DrivStockPriceUpdate d, MicrosoftStockPriceUpdate e)
    WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice AND c.PeakPrice < d.PeakPrice AND d.PeakPrice < e.PeakPrice
    WITHIN 10 minutes
    """
    msftDrivRacePattern = Pattern(
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
    workload_framework = ParallelTreeWorkloadFrameworkImplementation(msftDrivRacePattern, execution_units=1, is_data_parallel=False,
                                                                     is_structure_parallel=True, num_of_families=0)

    print("Running test for multiple structures, single data part with 1 structures, 1 data part")

    runTest('msftDrivRace', [msftDrivRacePattern], createTestFile, work_load_fr=workload_framework)


def MultipleStructuresSingleData5(createTestFile=False):
    """
    This pattern is looking for a race between driv and microsoft in ten minutes
    PATTERN SEQ(MicrosoftStockPriceUpdate a, DrivStockPriceUpdate b, MicrosoftStockPriceUpdate c, DrivStockPriceUpdate d, MicrosoftStockPriceUpdate e)
    WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice AND c.PeakPrice < d.PeakPrice AND d.PeakPrice < e.PeakPrice
    WITHIN 10 minutes
    """
    msftDrivRacePattern = Pattern(
        SeqOperator([PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"),
                     PrimitiveEventStructure("MSFT", "c"), PrimitiveEventStructure("DRIV", "d"),
                     PrimitiveEventStructure("MSFT", "e")]),
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
    workload_framework = ParallelTreeWorkloadFrameworkImplementation(msftDrivRacePattern, execution_units=3,
                                                                     is_data_parallel=False,
                                                                     is_structure_parallel=True, num_of_families=0)

    print("Running test for multiple structures, single data part with 3 structures, 1 data part")

    runTest('msftDrivRace', [msftDrivRacePattern], createTestFile, work_load_fr=workload_framework)


def MultipleStructuresSingleData6(createTestFile=False):
    """
    This pattern is looking for a race between driv and microsoft in ten minutes
    PATTERN SEQ(MicrosoftStockPriceUpdate a, DrivStockPriceUpdate b, MicrosoftStockPriceUpdate c, DrivStockPriceUpdate d, MicrosoftStockPriceUpdate e)
    WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice AND c.PeakPrice < d.PeakPrice AND d.PeakPrice < e.PeakPrice
    WITHIN 10 minutes
    """
    msftDrivRacePattern = Pattern(
        SeqOperator([PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"),
                     PrimitiveEventStructure("MSFT", "c"), PrimitiveEventStructure("DRIV", "d"),
                     PrimitiveEventStructure("MSFT", "e")]),
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
    workload_framework = ParallelTreeWorkloadFrameworkImplementation(msftDrivRacePattern, execution_units=7,
                                                                     is_data_parallel=False,
                                                                     is_structure_parallel=True, num_of_families=0)

    print("Running test for multiple structures, single data part with 5 structures, 1 data part")

    runTest('msftDrivRace', [msftDrivRacePattern], createTestFile, work_load_fr=workload_framework)

def MultipleStructuresSingleData7(createTestFile=False):
    pattern = Pattern(
        SeqOperator([NegationOperator(PrimitiveEventStructure("AAPL", "a")), PrimitiveEventStructure("AMAZON", "b"), NegationOperator(PrimitiveEventStructure("GOOG", "c")),
                     PrimitiveEventStructure("FB", "d"), NegationOperator(PrimitiveEventStructure("TYP1", "x"))]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )

    workload_framework = ParallelTreeWorkloadFrameworkImplementation(pattern, execution_units=1,
                                                                     is_data_parallel=False,
                                                                     is_structure_parallel=True, num_of_families=0)

    print("Running test for multiple structures, single data part with 1 structure, 1 data part")

    runTest("NotEverywhere", [pattern], createTestFile, work_load_fr=workload_framework)

def MultipleStructuresSingleData8(createTestFile=False):
    pattern = Pattern(
        SeqOperator([NegationOperator(PrimitiveEventStructure("AAPL", "a")), PrimitiveEventStructure("AMAZON", "b"), NegationOperator(PrimitiveEventStructure("GOOG", "c")),
                     PrimitiveEventStructure("FB", "d"), NegationOperator(PrimitiveEventStructure("TYP1", "x"))]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )

    workload_framework = ParallelTreeWorkloadFrameworkImplementation(pattern, execution_units=3,
                                                                     is_data_parallel=False,
                                                                     is_structure_parallel=True, num_of_families=0)

    print("Running test for multiple structures, single data part with 3 structures, 1 data part")

    runTest("NotEverywhere", [pattern], createTestFile, work_load_fr=workload_framework)

def MultipleStructuresSingleData9(createTestFile=False):
    pattern = Pattern(
        SeqOperator([NegationOperator(PrimitiveEventStructure("AAPL", "a")), PrimitiveEventStructure("AMAZON", "b"), NegationOperator(PrimitiveEventStructure("GOOG", "c")),
                     PrimitiveEventStructure("FB", "d"), NegationOperator(PrimitiveEventStructure("TYP1", "x"))]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )

    workload_framework = ParallelTreeWorkloadFrameworkImplementation(pattern, execution_units=7,
                                                                     is_data_parallel=False,
                                                                     is_structure_parallel=True, num_of_families=0)

    print("Running test for multiple structures, single data part with 5 structures, 1 data part")

    runTest("NotEverywhere", [pattern], createTestFile, work_load_fr=workload_framework)








