import itertools
from datetime import timedelta

from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, PrimitiveEventStructure
from condition.BaseRelationCondition import GreaterThanCondition
from condition.CompositeCondition import AndCondition
from condition.Condition import Variable
from plan.DeepTreeBuilder import DeepTreeBuilder
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

    tree_plan_build_approaches = [TreePlanBuilderOrder.LEFT_TREE, TreePlanBuilderOrder.RIGHT_TREE, TreePlanBuilderOrder.BALANCED_TREE]
    builders = {tree_plan_order: DeepTreeBuilder(tree_plan_order_approach=tree_plan_order) for tree_plan_order in tree_plan_build_approaches}

    multi_pattern_eval_approach = MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION
    first = True
    for approach1, approach2 in itertools.product(tree_plan_build_approaches, tree_plan_build_approaches):
        # print(approach1, approach2)
        builder1 = builders.get(approach1)
        builder2 = builders.get(approach2)
        tree1 = builder1.build_tree_plan(pattern1)
        tree2 = builder2.build_tree_plan(pattern2)
        pattern_to_tree_plan_map = {pattern1: tree1, pattern2: tree2}
        if first:
            print(f' tree 1 size: {builder1._sub_tree_size(tree1.root)}')
            print(f' tree 2 size: {builder2._sub_tree_size(tree2.root)}')
            print('=====================================================')

            first = False
        unified_tree = builder1._union_tree_plans(pattern_to_tree_plan_map.copy(), multi_pattern_eval_approach)
        builder1.visualize(visualize_data=tree1.root, title=f'{split(approach1)}')
        print(f' {split(approach1)}\t, {split(approach2)} : max intersection size : {builder1.trees_number_nodes_shared}')


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

    tree_plan_build_approaches = [TreePlanBuilderOrder.LEFT_TREE,
                                  TreePlanBuilderOrder.RIGHT_TREE,
                                  TreePlanBuilderOrder.BALANCED_TREE,
                                  TreePlanBuilderOrder.HALF_LEFT_HALF_BALANCED_TREE]
    builders = {tree_plan_order: DeepTreeBuilder(tree_plan_order_approach=tree_plan_order) for tree_plan_order in tree_plan_build_approaches}

    multi_pattern_eval_approach = MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION
    for approach in tree_plan_build_approaches:
        builder1 = builders.get(approach)
        tree1 = builder1.build_tree_plan(pattern1)
        pattern_to_tree_plan_map = {pattern1: tree1}
        unified_tree = builder1._union_tree_plans(pattern_to_tree_plan_map.copy(), multi_pattern_eval_approach)
        builder1.visualize(visualize_data=tree1.root, title=f' {split(approach)}')


if __name__ == '__main__':
    trees_number_nodes_shared()
    # visualize_build_approaches()
