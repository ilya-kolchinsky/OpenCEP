from test.testUtils import *
from datetime import timedelta
from base.Formula import GreaterThanFormula, SmallerThanFormula, IdentifierTerm, AndFormula, \
    NaryFormula, KCIndexFormula, KCValueFormula
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure, KleeneClosureOperator
from base.Pattern import Pattern


def structuralTest1():
    """
    Seq([a, KC(And([KC(d), KC(Seq([e, f]))]))])
    """
    structural_test_pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("GOOG", "a"),
                     KleeneClosureOperator(
                         AndOperator([PrimitiveEventStructure("GOOG", "b"),
                                      KleeneClosureOperator(PrimitiveEventStructure("GOOG", "c"),
                                                            min_size=1, max_size=5),
                                      KleeneClosureOperator(SeqOperator([PrimitiveEventStructure("GOOG", "d"), PrimitiveEventStructure("GOOG", "e")]),
                                                            min_size=1, max_size=5)]
                                     ),
                         min_size=1, max_size=5,
                     )]),
        AndFormula([
            NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 135),
            NaryFormula(IdentifierTerm("b", lambda x: x["Opening Price"]), relation_op=lambda x: x > 135)
        ]),
        timedelta(minutes=3)
    )

    expected_result = ('Seq', 'a', ('KC', ('And', ('And', 'b', ('KC', 'c')), ('KC', ('Seq', 'd', 'e')))))
    runStructuralTest('structuralTest1', [structural_test_pattern], expected_result)


def structuralTest2():
    """
    KC(a)
    """
    structural_test_pattern = Pattern(
        KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a")),
        AndFormula([
            NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 135)
        ]),
        timedelta(minutes=3)
    )
    expected_result = ('KC', 'a')
    runStructuralTest('structuralTest2', [structural_test_pattern], expected_result)


def structuralTest3():
    """
    Seq([a, KC(b)])
    """
    structural_test_pattern = Pattern(
        SeqOperator([
            PrimitiveEventStructure("GOOG", "a"), KleeneClosureOperator(PrimitiveEventStructure("GOOG", "b"))
        ]),
        AndFormula([
            NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 135)
        ]),
        timedelta(minutes=3)
    )
    expected_result = ('Seq', 'a', ('KC', 'b'))
    runStructuralTest('structuralTest3', [structural_test_pattern], expected_result)


def structuralTest4():
    """
    And([KC(a), b])
    """
    structural_test_pattern = Pattern(
        AndOperator([
            KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a")), PrimitiveEventStructure("GOOG", "b")
        ]),
        AndFormula([
            NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 135)
        ]),
        timedelta(minutes=3)
    )
    expected_result = ('And', ('KC', 'a'), 'b')
    runStructuralTest('structuralTest4', [structural_test_pattern], expected_result)


def structuralTest5():
    """
    KC(Seq([KC(a), KC(b)]))
    """
    structural_test_pattern = Pattern(
        KleeneClosureOperator(
            SeqOperator([
                KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"), min_size=3, max_size=5),
                KleeneClosureOperator(PrimitiveEventStructure("GOOG", "b"))
            ]), min_size=1, max_size=3
        ),
        AndFormula([
            NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 135)
        ]),
        timedelta(minutes=3)
    )
    expected_result = ('KC', ('Seq', ('KC', 'a'), ('KC', 'b')))
    runStructuralTest('structuralTest5', [structural_test_pattern], expected_result)


def structuralTest6():
    """
    Seq([a, Seq([ b, And([c, d]), e])])
    """
    structural_test_pattern = Pattern(
        SeqOperator([
            PrimitiveEventStructure("GOOG", "a"),
            SeqOperator([
                PrimitiveEventStructure("GOOG", "b"),
                AndOperator([
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("GOOG", "d")
                ]),
                PrimitiveEventStructure("GOOG", "e")
            ]),
        ]),
        AndFormula([
            NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 135)
        ]),
        timedelta(minutes=3)
    )
    expected_result = ('Seq', 'a', ('Seq', ('Seq', 'b', ('And', 'c', 'd')), 'e'))
    runStructuralTest('structuralTest6', [structural_test_pattern], expected_result)


def structuralTest7():
    """
    And([a, b, c, Seq([
                        d, KC(And([
                                e, KC(f), g
                              ]),
                        And([ KC(h), KC(Seq([ i, j])
                        ])
                   ]),
        k, l
    ])
    """
    structural_test_pattern = Pattern(
        AndOperator([
            PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b"), PrimitiveEventStructure("GOOG", "c"),
            SeqOperator([
                PrimitiveEventStructure("GOOG", "d"),
                KleeneClosureOperator(
                    AndOperator([
                        PrimitiveEventStructure("GOOG", "e"), KleeneClosureOperator(PrimitiveEventStructure("GOOG", "f")), PrimitiveEventStructure("GOOG", "g")
                    ])
                ), AndOperator([
                    KleeneClosureOperator(PrimitiveEventStructure("GOOG", "h")),
                    KleeneClosureOperator(
                        SeqOperator([
                            PrimitiveEventStructure("GOOG", "i"), PrimitiveEventStructure("GOOG", "j")
                        ]),
                    ),
                ]),
            ]),
            PrimitiveEventStructure("GOOG", "k"), PrimitiveEventStructure("GOOG", "l")
        ]),
        AndFormula([
            NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 135)
        ]),
        timedelta(minutes=3)
    )
    expected_result = ('And', ('And', ('And', ('And', ('And', 'a', 'b'), 'c'),
                                       ('Seq', ('Seq', 'd', ('KC', ('And', ('And', 'e', ('KC', 'f')), 'g'))),
                                        ('And', ('KC', 'h'), ('KC', ('Seq', 'i', 'j'))))), 'k'), 'l')
    runStructuralTest('structuralTest7', [structural_test_pattern], expected_result)


"""
identical to the first test in the file, with 1 exception - the PrimitiveEventStructure object is wrapped with a KC operator
"""
def oneArgumentsearchTestKleeneClosure(createTestFile=False):
    pattern = Pattern(
        SeqOperator([KleeneClosureOperator(PrimitiveEventStructure("AAPL", "a"), min_size=1, max_size=5)]),
        AndFormula([NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 135)]),
        timedelta(minutes=5)
    )
    runTest("oneArgumentKC", [pattern], createTestFile)


def MinMax_0_TestKleeneClosure(createTestFile=False):
    pattern = Pattern(
        SeqOperator([KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"), min_size=1, max_size=2)]),
        AndFormula([NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 0)]),
        timedelta(minutes=5)
    )
    runTest("MinMax_0_", [pattern], createTestFile, events=nasdaqEventStreamKC)

def MinMax_1_TestKleeneClosure(createTestFile=False):
    pattern = Pattern(
        SeqOperator([KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"))]),
        AndFormula([NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 0)]),
        timedelta(minutes=5)
    )
    runTest("MinMax_1_", [pattern], createTestFile, events=nasdaqEventStreamKC)

def MinMax_2_TestKleeneClosure(createTestFile=False):
    pattern = Pattern(
        SeqOperator([KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"), min_size=4, max_size=5)]),
        AndFormula([NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 0)]),
        timedelta(minutes=5)
    )
    runTest("MinMax_2_", [pattern], createTestFile, events=nasdaqEventStreamKC)


def KC_AND(createTestFile=False):
    """
    KC(And([a, b, c]))
    """
    pattern = Pattern(
        KleeneClosureOperator(
            AndOperator([
                PrimitiveEventStructure("GOOG", "a"),
                PrimitiveEventStructure("GOOG", "b"),
                PrimitiveEventStructure("GOOG", "c")
            ]), min_size=1, max_size=3
        ),
        AndFormula([
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"])),
        ]),
        timedelta(minutes=3)
    )
    runTest("KC_AND_", [pattern], createTestFile, events=nasdaqEventStreamKC)


def KC_AND_IndexFormula_01(createTestFile=False):
    """
    KC(And([a, b]))
    """
    pattern = Pattern(
        KleeneClosureOperator(
            AndOperator([
                PrimitiveEventStructure("GOOG", "a"),
                PrimitiveEventStructure("GOOG", "b")
            ]), min_size=1, max_size=3
        ),
        AndFormula([
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
            KCIndexFormula(names={'a', 'b'}, getattr_func=lambda x: x["Peak Price"], relation_op=lambda x, y: x < y,
                           index_1=0, index_2=2),
        ]),
        timedelta(minutes=3)
    )
    runTest("KC_AND_IndexFormula_01_", [pattern], createTestFile, events=nasdaqEventStreamKC)



def KC_AND_IndexFormula_02(createTestFile=False):
    """
    KC(And([a, b]))
    """
    pattern = Pattern(
        KleeneClosureOperator(
            AndOperator([
                PrimitiveEventStructure("GOOG", "a"),
                PrimitiveEventStructure("GOOG", "b")
            ]), min_size=1, max_size=3
        ),
        AndFormula([
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
            KCIndexFormula(names={'a', 'b'}, getattr_func=lambda x: x["Peak Price"], relation_op=lambda x, y: x < y,
                           offset=2),
        ]),
        timedelta(minutes=3)
    )
    runTest("KC_AND_IndexFormula_02_", [pattern], createTestFile, events=nasdaqEventStreamKC)


def KC_AND_NegOffSet_01(createTestFile=False):
    """
    KC(And([a, b, c]))
    """
    pattern = Pattern(
        KleeneClosureOperator(
            AndOperator([
                PrimitiveEventStructure("GOOG", "a"),
                PrimitiveEventStructure("GOOG", "b"),
                PrimitiveEventStructure("GOOG", "c")
            ]), min_size=1, max_size=3
        ),
        AndFormula([
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"])),
            KCIndexFormula(names={'a', 'b', 'c'}, getattr_func=lambda x: x["Peak Price"], relation_op=lambda x, y: x < 1 + y,
            offset=-1)
        ]),
        timedelta(minutes=3)
    )
    runTest("KC_AND_NegOffSet_01_", [pattern], createTestFile, events=nasdaqEventStreamKC)


def KC_AllValues(createTestFile=False):
    pattern = Pattern(
        SeqOperator([KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"))]),
        AndFormula([
            NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 0),
            KCValueFormula(names={'a'}, getattr_func=lambda x: x["Peak Price"], relation_op=lambda x, y: x > y, value=530.5)
            ]),
        timedelta(minutes=5)
    )
    runTest("KC_AllValues_01_", [pattern], createTestFile, events=nasdaqEventStreamKC)


def KC_Specific_Value(createTestFile=False):
    pattern = Pattern(
        SeqOperator([KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"))]),
        AndFormula([
            NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 0),
            KCValueFormula(names={'a'}, getattr_func=lambda x: x["Peak Price"], relation_op=lambda x, y: x > y, index=2, value=530.5)
            ]),
        timedelta(minutes=5)
    )
    runTest("KC_Specific_Value_", [pattern], createTestFile, events=nasdaqEventStreamKC)

def KC_Mixed(createTestFile=False):
    pattern = Pattern(
        SeqOperator([KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"))]),
        AndFormula([
            NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 0),
            KCValueFormula(names={'a'}, getattr_func=lambda x: x["Peak Price"],
                           relation_op=lambda x, y: x > y,
                           value=530.5),
            KCIndexFormula(names={'a'}, getattr_func=lambda x: x["Opening Price"],
                           relation_op=lambda x, y: x+0.5 < y,
                           offset=-1)
        ]),
        timedelta(minutes=5)
    )
    runTest("KC_Mixed_", [pattern], createTestFile, events=nasdaqEventStreamKC)


def KC_Formula_Failure_01(createTestFile=False):
    """
    KC(And([a, b, c]))
    """
    try:
        pattern = Pattern(
            KleeneClosureOperator(
                AndOperator([
                    PrimitiveEventStructure("GOOG", "a"),
                    PrimitiveEventStructure("GOOG", "b"),
                    PrimitiveEventStructure("GOOG", "c")
                ]), min_size=1, max_size=3
            ),
            AndFormula([
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"])),
                KCIndexFormula(names={'a', 'b', 'c'}, getattr_func=lambda x: x["Peak Price"],
                               relation_op=lambda x, y: x < 1 + y,
                               offset=-1, index_1=1, index_2=2)
            ]),
            timedelta(minutes=3)
        )
    except Exception as e:
        print("Test KC_Formula_Failure_01 Succeeded")
        return
    print("Test KC_Formula_Failure_01 Failed")


def KC_Formula_Failure_02(createTestFile=False):
    """
    KC(And([a, b, c]))
    """
    try:
        pattern = Pattern(
            KleeneClosureOperator(
                AndOperator([
                    PrimitiveEventStructure("GOOG", "a"),
                    PrimitiveEventStructure("GOOG", "b"),
                    PrimitiveEventStructure("GOOG", "c")
                ]), min_size=1, max_size=3
            ),
            AndFormula([
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"])),
                KCIndexFormula(names={'a', 'b', 'c'}, getattr_func=lambda x: x["Peak Price"],
                               relation_op=lambda x, y: x < 1 + y,
                               offset=-1, index_2=2)
            ]),
            timedelta(minutes=3)
        )
    except Exception as e:
        print("Test KC_Formula_Failure_02 Succeeded")
        return
    print("Test KC_Formula_Failure_02 Failed")


def KC_Formula_Failure_03(createTestFile=False):
    """
    KC(And([a, b, c]))
    """
    try:
        pattern = Pattern(
            KleeneClosureOperator(
                AndOperator([
                    PrimitiveEventStructure("GOOG", "a"),
                    PrimitiveEventStructure("GOOG", "b"),
                    PrimitiveEventStructure("GOOG", "c")
                ]), min_size=1, max_size=3
            ),
            AndFormula([
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"])),
                KCIndexFormula(names={'a', 'b', 'c'}, getattr_func=lambda x: x["Peak Price"],
                               relation_op=lambda x, y: x < 1 + y,
                               offset=-1, index_1=2)
            ]),
            timedelta(minutes=3)
        )
    except Exception as e:
        print("Test KC_Formula_Failure_03 Succeeded")
        return
    print("Test KC_Formula_Failure_03 Failed")
