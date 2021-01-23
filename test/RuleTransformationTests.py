from test.testUtils import *
from datetime import timedelta
from base.Pattern import Pattern
from condition.Condition import Variable
from condition.Condition import TrueCondition
from condition.BaseRelationCondition import GreaterThanEqCondition
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure, NegationOperator, OrOperator

from base.RuleTransformation import RuleTransformation

def tmpWorkTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        TrueCondition(),
        timedelta(minutes=120)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    NegationOperator(PrimitiveEventStructure("AMZN", "z")),
                    PrimitiveEventStructure("GOOG", "g"),
                    AndOperator(
                        PrimitiveEventStructure("AMZN", "z"), NegationOperator(PrimitiveEventStructure("GOOG", "g"))
                        )
                    ),
        TrueCondition(),
        timedelta(minutes=5)
    )

    pattern22 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    NegationOperator(PrimitiveEventStructure("AMZN", "z")),
                    PrimitiveEventStructure("GOOG", "g"),
                    AndOperator(
                        PrimitiveEventStructure("AMZN", "z"), NegationOperator(PrimitiveEventStructure("GOOG", "g")),
                        AndOperator(
                            PrimitiveEventStructure("AMZN", "zz"), PrimitiveEventStructure("GOOG", "gg")
                        )
                    )
                    ),
        TrueCondition(),
        timedelta(minutes=5)
    )

    pattern3 = Pattern(
        AndOperator(NegationOperator(PrimitiveEventStructure("AMZN", "z")), OrOperator(PrimitiveEventStructure("GOOG", "g"),PrimitiveEventStructure("AAPL", "a"))),
        TrueCondition(),
        timedelta(minutes=5)
    )

    pattern4 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a"),
                    NegationOperator(PrimitiveEventStructure("AMZN", "z")),
                    PrimitiveEventStructure("GOOG", "g"),
                    AndOperator(
                        PrimitiveEventStructure("AMZN", "zz"),
                        NegationOperator(PrimitiveEventStructure("GOOG", "gg")),
                        AndOperator(
                            PrimitiveEventStructure("AMZN", "zzz"),
                            NegationOperator(PrimitiveEventStructure("GOOG", "ggg"))
                        )
                    )
                    ),
        TrueCondition(),
        timedelta(minutes=5)
    )

    pattern5 = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("AMZN", "z")),
                    OrOperator(PrimitiveEventStructure("GOOG", "g"), PrimitiveEventStructure("AAPL", "a"))),
        TrueCondition(),
        timedelta(minutes=5)
    )

    # print(pattern2.extract_flat_sequences())
    trans_patterns = RuleTransformation.pattern_transformation(pattern4)
    # RuleTransformation.print_rule_transformation(trans_pattern)

    print()
    print("########")
    print(pattern4.full_structure)

    for trans_pattern in trans_patterns:
        print(trans_pattern.full_structure)
    print("########")
    print()

    # priority_list = RuleTransformation.get_priority_list()
    # for item in priority_list:
    #     print (item)
    # print()

    # runTest("one", [pattern], createTestFile)

def andAndPatternTransformationTest(createTestFile=False):
    pattern = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a"),
                    NegationOperator(PrimitiveEventStructure("AMZN", "z")),
                    PrimitiveEventStructure("GOOG", "g"),
                    AndOperator(
                        PrimitiveEventStructure("AMZN", "zz"),
                        NegationOperator(PrimitiveEventStructure("GOOG", "gg")),
                        AndOperator(
                            PrimitiveEventStructure("AMZN", "zzz"),
                            NegationOperator(PrimitiveEventStructure("GOOG", "ggg"))
                        )
                    )
                    ),
        TrueCondition(),
        # GreaterThanEqCondition(Variable("a", lambda x: x["Peak Price"]), 135),
        timedelta(minutes=5)
    )
    transformed_patterns = RuleTransformation.pattern_transformation(pattern)
    print (pattern.full_structure)
    for trans in transformed_patterns:
        print (trans.full_structure)
    print()
    # runTest("andAndPatternTransformation", transformed_patterns, createTestFile)
    # createExpectedOutput("andAndPatternTransformation", transformed_patterns)

def seqOrPatternTransformationTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("AMZN", "z")),
                    OrOperator(PrimitiveEventStructure("GOOG", "g"), PrimitiveEventStructure("AAPL", "a"))),
        GreaterThanEqCondition(Variable("g", lambda x: x["Peak Price"]), 135),
        timedelta(minutes=5)
    )
    transformed_patterns = RuleTransformation.pattern_transformation(pattern)
    print(pattern.full_structure)
    for trans in transformed_patterns:
        print (trans.full_structure)
    print()
    # runTest("seqOrPatternTransformation", transformed_patterns, createTestFile)
    # createExpectedOutput("seqOrPatternTransformation", transformed_patterns)