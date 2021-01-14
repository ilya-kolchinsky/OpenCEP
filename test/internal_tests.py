from test.testUtils import *
from datetime import timedelta
from condition.Condition import Variable, TrueCondition, BinaryCondition, SimpleCondition
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import EqCondition, GreaterThanCondition, GreaterThanEqCondition, SmallerThanEqCondition
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure
from base.Pattern import Pattern
from parallel.ParallelExecutionParameters import *

def stream_test():
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), 135),
        timedelta(minutes=0.5)
    )
    runTest(testName="stream", patterns=[pattern], createTestFile=False,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))
