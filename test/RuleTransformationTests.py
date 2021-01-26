from datetime import timedelta
from base.Pattern import Pattern
from condition.Condition import TrueCondition
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure, NegationOperator, OrOperator

from base.RuleTransformation import pattern_transformation


def ruleTransformationTests():
    andAndPatternTransformationTest()
    seqOrPatternTransformationTest()
    seqNotAndPatternTransformationTest()
    print ("Rule Transformation unit tests executed successfully.")

def andAndPatternTransformationTest():
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
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a"),
                    NegationOperator(PrimitiveEventStructure("AMZN", "z")),
                    PrimitiveEventStructure("GOOG", "g"),
                    PrimitiveEventStructure("AMZN", "zz"),
                    NegationOperator(PrimitiveEventStructure("GOOG", "gg")),
                    PrimitiveEventStructure("AMZN", "zzz"),
                    NegationOperator(PrimitiveEventStructure("GOOG", "ggg"))
                    ),
        TrueCondition(),
        timedelta(minutes=5)
    )
    transformed_patterns = pattern_transformation(pattern)
    assert pattern2.full_structure == transformed_patterns[0].full_structure, "Test andAndPatternTransformation Failed"

def seqOrPatternTransformationTest():
    pattern = Pattern(
        SeqOperator(NegationOperator(PrimitiveEventStructure("AMZN", "z")),
                    OrOperator(PrimitiveEventStructure("GOOG", "g"), PrimitiveEventStructure("AAPL", "a")),
                    OrOperator(PrimitiveEventStructure("GOOG", "gg"), PrimitiveEventStructure("AAPL", "aa"))),
        TrueCondition(),
        timedelta(minutes=5)
    )
    transformed_patterns = pattern_transformation(pattern)
    pattern_list = [
        Pattern(
            SeqOperator(NegationOperator(PrimitiveEventStructure("AMZN", "z")),
                        PrimitiveEventStructure("GOOG", "g"),
                        PrimitiveEventStructure("GOOG", "gg")),
            TrueCondition(),
            timedelta(minutes=5)
        ),
        Pattern(
            SeqOperator(NegationOperator(PrimitiveEventStructure("AMZN", "z")),
                        PrimitiveEventStructure("AAPL", "a"),
                        PrimitiveEventStructure("AAPL", "gg")),
            TrueCondition(),
            timedelta(minutes=5)
        ),
        Pattern(
            SeqOperator(NegationOperator(PrimitiveEventStructure("AMZN", "z")),
                        PrimitiveEventStructure("GOOG", "g"),
                        PrimitiveEventStructure("AAPL", "aa")),
            TrueCondition(),
            timedelta(minutes=5)
        ),
        Pattern(
            SeqOperator(NegationOperator(PrimitiveEventStructure("AMZN", "z")),
                        PrimitiveEventStructure("AAPL", "a"),
                        PrimitiveEventStructure("AAPL", "aa")),
            TrueCondition(),
            timedelta(minutes=5)
        )
    ]
    i = 0
    for transformed_pattern in transformed_patterns:
        assert transformed_pattern.full_structure == pattern_list[i].full_structure, "Test seqOrPatternTransformation Failed"
        i = i + 1

def seqNotAndPatternTransformationTest():
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    NegationOperator(AndOperator(
                        PrimitiveEventStructure("AMZN", "z"),
                        PrimitiveEventStructure("GOOG", "g"))),
                    PrimitiveEventStructure("MSFT", "m")
                    ),
        TrueCondition(),
        timedelta(minutes=5)
    )
    transformed_patterns = pattern_transformation(pattern)
    pattern_list = [
        Pattern(
            SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                        NegationOperator(PrimitiveEventStructure("AMZN", "z")),
                        PrimitiveEventStructure("MSFT", "m")),
            TrueCondition(),
            timedelta(minutes=5)
        ),
        Pattern(
            SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                        NegationOperator(PrimitiveEventStructure("GOOG", "g")),
                        PrimitiveEventStructure("MSFT", "m")),
            TrueCondition(),
            timedelta(minutes=5)
        )
    ]
    i = 0
    for transformed_pattern in transformed_patterns:
        assert transformed_pattern.full_structure == pattern_list[i].full_structure, "Test seqNotAndPatternTransformation Failed"
        i = i + 1