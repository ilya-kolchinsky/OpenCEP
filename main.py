import itertools
from datetime import timedelta

from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, PrimitiveEventStructure
from condition.BaseRelationCondition import GreaterThanCondition
from condition.CompositeCondition import AndCondition
from condition.Condition import Variable
from plan.TreePlanBuilderOrders import TreePlanBuilderOrder
from plan.multi.MultiPatternUnifiedTreePlanApproaches import MultiPatternTreePlanUnionApproaches
from test.testUtils import *

approaches = {
    1: TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
    2: TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE,
    3: TreePlanBuilderTypes.GREEDY_LEFT_DEEP_TREE,
    4: TreePlanBuilderTypes.LOCAL_SEARCH_LEFT_DEEP_TREE,
    5: TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE,
    6: TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE,
    7: TreePlanBuilderTypes.ZSTREAM_BUSHY_TREE,
    8: TreePlanBuilderTypes.ORDERED_ZSTREAM_BUSHY_TREE}


def split(string):
    return '{:10s}'.format(str(string).split(".")[1].split("_TREE")[0].replace("_", " "))


def trees_number_nodes_shared():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("GOOG", "e")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("c", lambda x: x["Peak Price"]), 503)
        ),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("GOOG", "e")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("c", lambda x: x["Peak Price"]), 503)
        ),
        timedelta(minutes=3)
    )



def visualize_build_approaches():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("GOOG", "d"),
                    PrimitiveEventStructure("GOOG", "e"),
                    PrimitiveEventStructure("GOOG", "f"),
                    PrimitiveEventStructure("GOOG", "g"),
                    ),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("c", lambda x: x["Peak Price"]), 500)
        ),
        timedelta(minutes=5)
    )


if __name__ == '__main__':
    trees_number_nodes_shared()
    # visualize_build_approaches()
