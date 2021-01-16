import itertools
import random
from datetime import timedelta
from itertools import combinations
from typing import List, Dict, Tuple

import numpy as np

from SimulatedAnnealing import tree_plan_visualize_annealing
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, OrOperator, AndOperator, KleeneClosureOperator, NegationOperator, \
    PrimitiveEventStructure
from condition.BaseRelationCondition import GreaterThanCondition, SmallerThanCondition
from condition.CompositeCondition import AndCondition
from condition.Condition import Variable
from evaluation.EvaluationMechanismFactory import TreeBasedEvaluationMechanismParameters
from misc.StatisticsTypes import StatisticsTypes
from misc.Utils import get_all_disjoint_sets
from plan import TreeCostModels
from plan.LeftDeepTreeBuilders import TrivialLeftDeepTreeBuilder
from plan.TreeCostModel import TreeCostModelFactory
from plan.TreePlan import *
from plan.TreePlanBuilder import TreePlanBuilder
from plan.TreePlanBuilderFactory import TreePlanBuilderFactory
from plan.UnifiedTreeBuilder import UnifiedTreeBuilder


class algoA(TreePlanBuilder):

    def _create_tree_topology(self, pattern: Pattern):

        if pattern.statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            (selectivity_matrix, arrival_rates) = pattern.statistics
        else:
            builder = UnifiedTreeBuilder.get_instance()
            pattern_tree_plan = builder.build_tree_plan(pattern)
            return pattern_tree_plan

        args_num = len(selectivity_matrix)
        if args_num == 1:
            assert len(pattern.positive_structure.get_args()) == 1
            event_index = pattern.get_index_by_event_name(pattern.positive_structure.get_args()[0])
            node = TreePlanLeafNode(event_index)
            return TreePlan(root=node)

        items = frozenset(range(args_num))
        # Save subsets' optimal topologies, the cost and the left to add items.
        sub_trees = {frozenset({i}): (TreePlanLeafNode(i),
                                      self._get_plan_cost(pattern, TreePlanLeafNode(i)),
                                      items.difference({i}))
                     for i in items}

        # for each subset of size i, find optimal topology for these subsets according to size (i-1) subsets.
        for i in range(2, args_num + 1):
            for tSubset in combinations(items, i):
                subset = frozenset(tSubset)
                disjoint_sets_iter = get_all_disjoint_sets(subset)  # iterator for all disjoint splits of a set.
                # use first option as speculative best.
                set1_, set2_ = next(disjoint_sets_iter)
                tree1_, _, _ = sub_trees[set1_]
                tree2_, _, _ = sub_trees[set2_]
                new_tree_ = TreePlanBuilder._instantiate_binary_node(pattern, tree1_, tree2_)
                new_cost_ = self._get_plan_cost(pattern, new_tree_)
                new_left_ = items.difference({subset})
                sub_trees[subset] = new_tree_, new_cost_, new_left_
                # find the best topology based on previous topologies for smaller subsets.
                for set1, set2 in disjoint_sets_iter:
                    tree1, _, _ = sub_trees[set1]
                    tree2, _, _ = sub_trees[set2]
                    new_tree = TreePlanBuilder._instantiate_binary_node(pattern, tree1, tree2)
                    new_cost = self._get_plan_cost(pattern, new_tree)
                    _, cost, left = sub_trees[subset]
                    # if new subset's topology is better, then update to it.
                    if new_cost < cost:
                        sub_trees[subset] = new_tree, new_cost, left
        return sub_trees[items][0]  # return the best topology (index 0 at tuple) for items - the set of all arguments.

    def _create_topology_with_const_sub_order(self, pattern: Pattern, const_sub_ord: list):
        if pattern.statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            (selectivity_matrix, arrival_rates) = pattern.statistics
        else:
            return TrivialLeftDeepTreeBuilder(TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL)
        args_num = len(selectivity_matrix)
        if args_num == 1:
            pattern_event_name = {event.name for event in pattern.positive_structure.get_args()}
            assert len(pattern_event_name) == 1
            event_index = pattern.get_index_by_event_name(pattern_event_name[0])
            return TreePlanLeafNode(event_index)

        items = frozenset(range(args_num)) - frozenset(const_sub_ord)
        # Save subsets' optimal topologies, the cost and the left to add items.
        sub_trees = {frozenset({i}): (TreePlanLeafNode(i),
                                      self._get_plan_cost(pattern, TreePlanLeafNode(i)),
                                      items.difference({i}))
                     for i in items}

        # for each subset of size i, find optimal topology for these subsets according to size (i-1) subsets.
        for i in range(2, args_num + 1):
            for tSubset in combinations(items, i):
                subset = frozenset(tSubset)
                disjoint_sets_iter = get_all_disjoint_sets(subset)  # iterator for all disjoint splits of a set.
                # use first option as speculative best.
                set1_, set2_ = next(disjoint_sets_iter)
                tree1_, _, _ = sub_trees[set1_]
                tree2_, _, _ = sub_trees[set2_]
                new_tree_ = TreePlanBuilder._instantiate_binary_node(pattern, tree1_, tree2_)
                new_cost_ = self._get_plan_cost(pattern, new_tree_)
                new_left_ = items.difference({subset})
                sub_trees[subset] = new_tree_, new_cost_, new_left_
                # find the best topology based on previous topologies for smaller subsets.
                for set1, set2 in disjoint_sets_iter:
                    tree1, _, _ = sub_trees[set1]
                    tree2, _, _ = sub_trees[set2]
                    new_tree = TreePlanBuilder._instantiate_binary_node(pattern, tree1, tree2)
                    new_cost = self._get_plan_cost(pattern, new_tree)
                    _, cost, left = sub_trees[subset]
                    # if new subset's topology is better, then update to it.
                    if new_cost < cost:
                        sub_trees[subset] = new_tree, new_cost, left
        return sub_trees[items][0]  # return the best topology (index 0 at tuple) for items - the set of all arguments.

    def _create_tree_topology_shared_subpattern(self, pattern: Pattern, sub_pattern: Pattern = None):
        """this function builds the best tree topology such that the pattern starts with the subpattern order
         and the remained pattern built with best order"""
        if sub_pattern is None:
            return self._create_topology_with_const_sub_order(pattern, [])
        sub_pattern_topology = self._create_topology_with_const_sub_order(sub_pattern, [])
        event_names = {event.name for event in sub_pattern.positive_structure.get_args()}
        sub_pattern_order = [sub_pattern.get_index_by_event_name(name) for name in event_names]
        complementary_pattern_topolgy = self._create_topology_with_const_sub_order(pattern, sub_pattern_order)
        return TreePlanBuilder._instantiate_binary_node(pattern, sub_pattern_topology, complementary_pattern_topolgy)

    @staticmethod
    def get_all_subtree_roots(plan_node: TreePlanNode):
        if isinstance(plan_node, TreePlanLeafNode):
            return [plan_node]
        elif isinstance(plan_node, TreePlanUnaryNode):
            return [plan_node] + algoA.get_all_subtree_roots(plan_node.child)
        else:
            assert isinstance(plan_node, TreePlanBinaryNode)
        return [plan_node] \
               + algoA.get_all_subtree_roots(plan_node.left_child) \
               + algoA.get_all_subtree_roots(plan_node.right_child)

    @staticmethod
    def get_event_args(node: TreePlanNode, pattern: Pattern):
        pattern_event_names = [event.name for event in pattern.positive_structure.get_args()]
        pattern_event_types = [event.type for event in pattern.positive_structure.get_args()]
        pattern_event_indexes = [pattern.get_index_by_event_name(name) for name in pattern_event_names]
        wanted_event_index = node.event_index
        index_in_list = pattern_event_indexes.index(wanted_event_index)
        return pattern_event_types[index_in_list], pattern_event_names[index_in_list]

    @staticmethod
    def build_pattern_from_plan_node(node: TreePlanNode, pattern1: Pattern, leaves_dict, first_time=False):
        if first_time:
            condition = UnifiedTreeBuilder.get_condition_from_pattern_in_one_sub_tree(node, pattern1, leaves_dict)
            return Pattern(algoA.build_pattern_from_plan_node(node, pattern1, leaves_dict), condition,
                           pattern1.window, pattern1.consumption_policy, pattern1.id)
        node_type = type(node)
        if issubclass(node_type, TreePlanLeafNode):
            leaves_in_plan_node_1 = node.get_leaves()
            assert len(leaves_in_plan_node_1) == 1

            event_type, event_name = algoA.get_event_args(node, pattern1)
            return PrimitiveEventStructure(event_type, event_name)
        elif issubclass(node_type, TreePlanInternalNode):  # internal node
            node_operator: OperatorTypes = node.get_operator()
            if node_operator == OperatorTypes.SEQ:
                return SeqOperator(algoA.build_pattern_from_plan_node(node.left_child, pattern1, leaves_dict),
                                   algoA.build_pattern_from_plan_node(node.right_child, pattern1, leaves_dict))
            elif node_operator == OperatorTypes.OR:
                return OrOperator(algoA.build_pattern_from_plan_node(node.left_child, pattern1, leaves_dict),
                                  algoA.build_pattern_from_plan_node(node.right_child, pattern1, leaves_dict))
            elif node_operator == OperatorTypes.AND:
                return AndOperator(algoA.build_pattern_from_plan_node(node.left_child, pattern1, leaves_dict),
                                   algoA.build_pattern_from_plan_node(node.right_child, pattern1, leaves_dict))
            elif node_operator == OperatorTypes.KC:
                return KleeneClosureOperator(
                    algoA.build_pattern_from_plan_node(node.child, pattern1, leaves_dict))
            elif node_operator == OperatorTypes.NSEQ:
                return NegationOperator(
                    SeqOperator(algoA.build_pattern_from_plan_node(node.left_child, pattern1, leaves_dict),
                                algoA.build_pattern_from_plan_node(node.right_child, pattern1, leaves_dict)))
            elif node_operator == OperatorTypes.NAND:
                return NegationOperator(
                    AndOperator(algoA.build_pattern_from_plan_node(node.left_child, pattern1, leaves_dict),
                                algoA.build_pattern_from_plan_node(node.right_child, pattern1, leaves_dict)))
            else:
                raise NotImplementedError

    @staticmethod
    def get_all_sharable_sub_patterns(tree_plan1: TreePlan, pattern1: Pattern, tree_plan2: TreePlan, pattern2: Pattern):
        tree1_subtrees = algoA.get_all_subtree_roots(tree_plan1.root)
        tree2_subtrees = algoA.get_all_subtree_roots(tree_plan2.root)
        sharable = []

        pattern_to_tree_plan_map = {pattern1: tree_plan1, pattern2: tree_plan2}

        leaves_dict = {}
        for i, pattern in enumerate(pattern_to_tree_plan_map):
            tree_plan_leaves_pattern = pattern_to_tree_plan_map[pattern].root.get_leaves()
            pattern_event_size = len(pattern.positive_structure.get_args())
            leaves_dict[pattern] = {tree_plan_leaves_pattern[i]: pattern.positive_structure.get_args()[i] for i in
                                    range(pattern_event_size)}

        for node1, node2 in itertools.product(tree1_subtrees, tree2_subtrees):
            if UnifiedTreeBuilder.is_equivalent(node1, pattern1, node2, pattern2, leaves_dict):
                sharable_sub_pattern = algoA.build_pattern_from_plan_node(node1, pattern1, leaves_dict, first_time=True)
                sharable_sub_pattern.set_time_window(min(pattern1.window,
                                                         pattern2.window))  # TODO : need to make sure which one to apply between min/max
                sharable.append(sharable_sub_pattern)
        return list(set(sharable))  # this conversion to set and back to list is to make sure we get no duplicates

    @staticmethod
    def get_shareable_pairs(pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan):
        number_of_patterns = len(pattern_to_tree_plan_map)
        shape = (number_of_patterns, number_of_patterns)
        shareable_pairs_array = np.empty(shape=shape, dtype=list)

        for i, patterni in enumerate(pattern_to_tree_plan_map.keys()):
            for j, patternj in enumerate(pattern_to_tree_plan_map.keys()):
                if j == i:
                    continue
                if j < i:
                    shareable_pairs_array[i][j] = shareable_pairs_array[j][i]
                    continue
                tree_plan1 = pattern_to_tree_plan_map[patterni]
                tree_plan2 = pattern_to_tree_plan_map[patternj]
                sharable_sub_patterns = algoA.get_all_sharable_sub_patterns(tree_plan1, patterni, tree_plan2, patternj)
                shareable_pairs_array[i][j] = sharable_sub_patterns
        return shareable_pairs_array

    # @staticmethod
    # def Nedge_neighborhood(pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan):
    #     if isinstance(pattern_to_tree_plan_map, TreePlan) or len(pattern_to_tree_plan_map) <= 1:
    #         return pattern_to_tree_plan_map
    #     # TODO : make sure pattern_to_tree_plan_map doesn't need copy
    #
    #     number_of_patterns = len(pattern_to_tree_plan_map)
    #     random_patterns_pair = random.choices(range(number_of_patterns), k=2)
    #     index1 = min(random_patterns_pair)
    #     index2 = max(random_patterns_pair)
    #
    #     for i, patterni in enumerate(pattern_to_tree_plan_map.keys()):
    #         for j, patternj in enumerate(pattern_to_tree_plan_map.keys()):
    #             if i >= j:
    #                 break
    #             tree_plan1 = pattern_to_tree_plan_map[patterni]
    #             tree_plan2 = pattern_to_tree_plan_map[patternj]
    #             sharable_sub_patterns = algoA.get_all_sharable_sub_patterns(tree_plan1, patternj, tree_plan2, patternj)
    #             if len(sharable_sub_patterns) > 0:
    #                 for sub_pattern in sharable_sub_patterns:
    #                     new_tree_plan1 = algoA._create_tree_topology_shared_subpattern(patterni, sub_pattern)
    #                     new_tree_plan2 = algoA._create_tree_topology_shared_subpattern(patternj, sub_pattern)
    #                     pattern_to_tree_plan_map[i] = new_tree_plan1
    #                     pattern_to_tree_plan_map[j] = new_tree_plan2

    @staticmethod
    def Nedge_neighborhood(pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan,
                           sub_pattern_shareable_array: np.array):
        """
        the Nedge neighborhood function as explained in the article Section 4.2

        :param pattern_to_tree_plan_map : dict that maps every pattern in the MPT to his TreePlan
        :param sub_pattern_shareable_array : a [n][n] matrix (n is number of patterns in MPT ) ,
                   where sub_pattern_shareable_array[i][j] = the list of all equal  sub patterns between patterni and patternj
        :return: creates new tree plans where sub pattern of two patterns merged

        """
        if isinstance(pattern_to_tree_plan_map, TreePlan) or len(pattern_to_tree_plan_map) <= 1:
            return pattern_to_tree_plan_map
        random_patterns_pair = random.choices(list(enumerate(pattern_to_tree_plan_map.items())), k=2)
        shareable_pairs_i_j = []
        i, j, patterni, patternj = 0, 0, None, None
        while len(shareable_pairs_i_j) == 0:
            i, patterni = random_patterns_pair[0]
            j, patternj = random_patterns_pair[1]

            shareable_pairs_i_j = sub_pattern_shareable_array[i, j]

        if len(shareable_pairs_i_j) == 0:
            """
            no share exist
            """
            return
        assert len(shareable_pairs_i_j) > 0
        sub_pattern = random.choices(shareable_pairs_i_j, k=1)[0]

        new_tree_plan1 = algoA._create_tree_topology_shared_subpattern(patterni, sub_pattern)
        new_tree_plan2 = algoA._create_tree_topology_shared_subpattern(patternj, sub_pattern)
        pattern_to_tree_plan_map[patterni] = new_tree_plan1
        pattern_to_tree_plan_map[patternj] = new_tree_plan2

        assert sub_pattern in sub_pattern_shareable_array[i, j]
        assert sub_pattern in sub_pattern_shareable_array[j, i]

        sub_pattern_shareable_array[i, j].remove(sub_pattern)
        sub_pattern_shareable_array[j, i].remove(sub_pattern)


def shareable_all_pairs_unit_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "x"), PrimitiveEventStructure("AMZN", "y"),
                    PrimitiveEventStructure("GOOG", "z")),
        AndCondition(
            GreaterThanCondition(Variable("x", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("x", lambda x: x["Opening Price"]),
                                 Variable("y", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("y", lambda x: x["Opening Price"]),
                                 Variable("z", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )

    patterns = [pattern1, pattern2]

    eval_mechanism_params = TreeBasedEvaluationMechanismParameters()
    tree_plan_builder = TreePlanBuilderFactory.create_tree_plan_builder(eval_mechanism_params.tree_plan_params)
    tree_plan = tree_plan_builder.build_tree_plan(pattern1)

    pattern_to_tree_plan_map = {p: tree_plan_builder.build_tree_plan(p) for p in patterns}

    shareable_pairs = algoA.get_all_sharable_sub_patterns(pattern_to_tree_plan_map[pattern1], pattern1,
                                                          pattern_to_tree_plan_map[pattern2], pattern2)
    print('Ok')

    sub_pattern1 = algoA.build_pattern_from_plan_node(node=tree_plan.root.left_child,
                                                      pattern1=pattern, first_time=True)

def create_topology_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("GOOG", "d")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern1.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    algoA_instance = algoA()
    new_plan = algoA_instance._create_topology_with_const_sub_order(pattern1, [0, 3])
    print('Ok')

# =======================================================================================
def tree_plan_cost_function(state: Tuple[Dict[Pattern, TreePlan], np.array]):
    pattern_to_tree_plan_map, _ = state
    cost_model = TreeCostModelFactory.create_cost_model()

    def _single_pattern_cost(pattern: Pattern, tree_plan_root: TreePlanNode):
        try:
            cost = cost_model.get_plan_cost(pattern, tree_plan_root)
            return cost
        except:
            return 0  # TODO??

    tree_plan_total_cost = sum([_single_pattern_cost(pattern, tree_plan.root) for pattern, tree_plan in pattern_to_tree_plan_map.items()])
    return tree_plan_total_cost


def patterns_initialize_function(patterns: List[Pattern]):
    alg = algoA()
    pattern_to_tree_plan_map = {p: alg._create_tree_topology(p) for p in patterns}
    shareable_pairs = algoA.get_shareable_pairs(pattern_to_tree_plan_map)
    return pattern_to_tree_plan_map, shareable_pairs


def tree_plan_neighbour(state: Tuple[Dict[Pattern, TreePlan], np.array]):
    """Move a little bit x, from the left or the right."""
    pattern_to_tree_plan_map, shareable_pairs = state
    alg = algoA()
    neighbour = alg.Nedge_neighborhood(pattern_to_tree_plan_map, shareable_pairs)
    return neighbour

def tree_plan_state_get_summary(state: Tuple[Dict[Pattern, TreePlan], np.array]):
    _, shareable_pairs = state
    count_common_pairs = lambda lst: len(lst) if lst is not None else 0

    # dividing by 2 cause the shareable_pairs is a symmetric matrix
    return "common pairs size = " + str(np.sum([count_common_pairs(lst) for lst in shareable_pairs.reshape(-1)]) // 2)


if __name__ == '__main__':
    # shareable_pairs_unit_test()

    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZ", "b")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZ", "b")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )

    patterns = [pattern1, pattern2]
    pattern_to_tree_plan_map = patterns_initialize_function(patterns)
    state, c, states, costs = tree_plan_visualize_annealing(patterns=patterns,
                                                            initialize_function=patterns_initialize_function,
                                                            state_repr_function=tree_plan_state_get_summary,
                                                            cost_function=tree_plan_cost_function,
                                                            neighbour_function=tree_plan_neighbour)


