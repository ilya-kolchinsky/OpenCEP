from test.testUtils import *


def structuralTest1():
    """
    Seq([a, KC(And([KC(d), KC(Seq([e, f]))]))])
    """
    structural_test_pattern = Pattern(
        SeqOperator([QItem("GOOG", "a"),
                     KleeneClosureOperator(
                         AndOperator([QItem("GOOG", "b"),
                                      KleeneClosureOperator(QItem("GOOG", "c"),
                                                            min_size=1, max_size=5),
                                      KleeneClosureOperator(SeqOperator([QItem("GOOG", "d"), QItem("GOOG", "e")]),
                                                            min_size=1, max_size=5)]
                                     ),
                         min_size=1, max_size=5,
                     )]),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )

    expected_result = ''
    runStructuralTest('structuralTest1', [structural_test_pattern], expected_result)


def structuralTest2():
    """
    KC(a)
    """
    structural_test_pattern = Pattern(
        KleeneClosureOperator(QItem("GOOG", "a")),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    expected_result = ''
    runStructuralTest('structuralTest2', [structural_test_pattern], expected_result)


def structuralTest3():
    """
    Seq([a, KC(b)])
    """
    structural_test_pattern = Pattern(
        SeqOperator([
            QItem("GOOG", "a"), KleeneClosureOperator(QItem("GOOG", "b"))
        ]),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    expected_result = ''
    runStructuralTest('structuralTest3', [structural_test_pattern], expected_result)


def structuralTest4():
    """
    And([KC(a), b])
    """
    structural_test_pattern = Pattern(
        AndOperator([
            KleeneClosureOperator(QItem("GOOG", "a")), QItem("GOOG", "b")
        ]),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    expected_result = ''
    runStructuralTest('structuralTest4', [structural_test_pattern], expected_result)


def structuralTest5():
    """
    KC(Seq([KC(a), KC(b)]))
    """
    structural_test_pattern = Pattern(
        KleeneClosureOperator(
            SeqOperator([
                KleeneClosureOperator(QItem("GOOG", "a"), min_size=3, max_size=5),
                KleeneClosureOperator(QItem("GOOG", "b"))  # default values of min=1, max=5 defined at PatternStructure.py
            ]), min_size=1, max_size=3
        ),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    expected_result = ''
    runStructuralTest('structuralTest5', [structural_test_pattern], expected_result)


def structuralTest6():
    """
    Seq([a, Seq([ b, And([c, d])])])
    """
    structural_test_pattern = Pattern(
        SeqOperator([
            QItem("GOOG", "a"),
            SeqOperator([
                QItem("GOOG", "b"),
                AndOperator([
                    QItem("GOOG", "c"),
                    QItem("GOOG", "d")
                ]),
                QItem("GOOG", "e")
            ]),
        ]),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    expected_result = ''
    runStructuralTest('structuralTest6', [structural_test_pattern], expected_result)


def structuralTest7():
    """
    And([a, b, c, Seq([
                        d, KC(And([
                                e, KC(f), g
                              ]),
                        And([ KC(h), KC(Seq([ i, j
                                        ])
                        ])
                   ]),
        k, l
    ])
    """
    structural_test_pattern = Pattern(
        AndOperator([
            QItem("GOOG", "a"), QItem("GOOG", "b"), QItem("GOOG", "c"),
            SeqOperator([
                QItem("GOOG", "d"),
                KleeneClosureOperator(
                    AndOperator([
                        QItem("GOOG", "e"), KleeneClosureOperator(QItem("GOOG", "f")), QItem("GOOG", "g")
                    ])
                ), AndOperator([
                    KleeneClosureOperator(QItem("GOOG", "h")),
                    KleeneClosureOperator(
                        SeqOperator([
                            QItem("GOOG", "i"), QItem("GOOG", "j")
                        ]),
                    ),
                ]),
            ]),
            QItem("GOOG", "k"), QItem("GOOG", "l")
        ]),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    expected_result = ''
    runStructuralTest('structuralTest7', [structural_test_pattern], expected_result)


"""
identical to the first test in the file, with 1 exception - the QItem object is wrapped with a KC operator
"""
def oneArgumentsearchTestKleeneClosure(createTestFile=False):
    pattern = Pattern(
        SeqOperator([KleeneClosureOperator(QItem("AAPL", "a"))]),
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), AtomicTerm(135)),
        timedelta(minutes=5)
    )
    runTest("oneArgumentKC", [pattern], createTestFile)


# ------------------------------------------------------
#       KleeneClosure tests
# ------------------------------------------------------


oneArgumentsearchTestKleeneClosure()


# ------------------------------------------------------
#   tests for the tree structure, CEP only created not used!.
# ------------------------------------------------------

# structuralTest1()
# structuralTest2()
# structuralTest3()
# structuralTest4()
# structuralTest5()
# structuralTest6()
# structuralTest7()
