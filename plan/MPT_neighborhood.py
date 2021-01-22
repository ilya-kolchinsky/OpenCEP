import itertools
import random
from copy import deepcopy
from datetime import timedelta
from itertools import combinations
from typing import List, Dict, Tuple

import numpy as np

from SimulatedAnnealing import tree_plan_visualize_annealing, TreeBasedEvaluationMechanism
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
from plan.TreeCostModel import TreeCostModelFactory, TreeCostModel
from plan.TreePlan import *
from plan.TreePlanBuilder import TreePlanBuilder
from plan.TreePlanBuilderFactory import TreePlanBuilderFactory
from plan.TreePlanBuilderOrders import TreePlanBuilderOrder
from plan.UnifiedTreeBuilder import UnifiedTreeBuilder


class algoA(TreePlanBuilder):

    def _create_tree_topology(self, pattern: Pattern):
        """
        Description: this function returns a treePlan for the given pattern
        we do that using dynamic programming , in each iteration we calculate the sub plan cost
        and pick the the one with the cheapest cost , the best sub plan is what we consider to build from for the next
        iterations , note that cost(plan) is calculated by the pattern statistics , if no statistics passed then there
         is no meaning to the cheapest plan , we simply build any plan and return it
        """
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
        return TreePlan(root=sub_trees[items][
            0])  # return the best topology (index 0 at tuple) for items - the set of all arguments.

    def _create_topology_with_const_sub_order(self, pattern: Pattern,
                                              const_sub_ord: list):
        """
        Description :the same as _create_tree_topology , only that this time we build the plan for the
        complementary pattern the complementary pattern is defined by the pattern events indexes minus
        the events indexes in const_sub_order
        """
        if pattern.statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            (selectivity_matrix, arrival_rates) = pattern.statistics
        else:
            builder = UnifiedTreeBuilder.get_instance()
            pattern_tree_plan = builder.build_tree_plan(pattern)
            return pattern_tree_plan

        if isinstance(pattern.full_structure, PrimitiveEventStructure):
            assert len(const_sub_ord) == 1
            event_index = const_sub_ord[0]
            node = TreePlanLeafNode(event_index)
            return node
        args_num = len(selectivity_matrix)
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

    def _create_pattern_topology(self, sub_pattern_data: Tuple):
        """
        Description: like create_tree_topology function , only that this time we pass the
        sub pattern data which is a tuble of 3 :  sub_pattern, sub_pattern event indexes, sub_pattern event names
        we use this information to build the sub_pattern we custom build where we choose what the events are in the sub tree
        because we want those same indexes of the big pattern that includes the sub pattern and not some new indexes
        """
        pattern, sub_pattern_indexes, sub_pattern_names = sub_pattern_data
        if pattern.statistics_type != StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            builder = UnifiedTreeBuilder.get_instance()
            pattern_tree_plan = builder.build_tree_plan(pattern)
            return pattern_tree_plan

        if isinstance(pattern.full_structure, PrimitiveEventStructure):
            assert len(sub_pattern_indexes) == 1
            event_index = sub_pattern_indexes[0]
            node = TreePlanLeafNode(event_index)
            return node

        # return TreePlan(root=node)
        args_num = len(sub_pattern_indexes)
        items = frozenset(sub_pattern_indexes)
        # Save subsets' optimal topologies, the cost and the left to add items.
        sub_trees = {frozenset({i}): (TreePlanLeafNode(i),
                                      self._get_plan_cost(pattern, TreePlanLeafNode(i)),
                                      items.difference({i}))
                     for i in sub_pattern_indexes}
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

    def _create_tree_topology_shared_subpattern(self, pattern: Pattern, sub_pattern_data: Tuple):
        """this function builds the best tree topology such that the pattern starts with the subpattern order
         and the remained pattern built with best order"""
        sub_pattern, sub_pattern_indexes, sub_pattern_names = sub_pattern_data
        if sub_pattern is None:
            return self._create_topology_with_const_sub_order(pattern, [])
        sub_pattern_topology = self._create_pattern_topology(sub_pattern_data)

        if len(pattern.full_structure.get_args()) == len(sub_pattern_indexes):
            return TreePlan(root=sub_pattern_topology)

        sub_pattern_order = sub_pattern_indexes
        complementary_pattern_topolgy = self._create_topology_with_const_sub_order(pattern, sub_pattern_order)
        node = TreePlanBuilder._instantiate_binary_node(pattern, sub_pattern_topology, complementary_pattern_topolgy)
        return TreePlan(root=node)

    @staticmethod
    def get_all_nodes(plan_node: TreePlanNode):
        """simply returns all plan nodes in the tree with the root = the argument plan node"""
        if isinstance(plan_node, TreePlanLeafNode):
            return [plan_node]
        elif isinstance(plan_node, TreePlanUnaryNode):
            return [plan_node] + algoA.get_all_nodes(plan_node.child)
        else:
            assert isinstance(plan_node, TreePlanBinaryNode)
        return [plan_node] \
               + algoA.get_all_nodes(plan_node.left_child) \
               + algoA.get_all_nodes(plan_node.right_child)

    @staticmethod
    def get_event_args(node: TreePlanNode, pattern: Pattern):
        """ Discription: we pass a leaf node that got event index/type/name , we return all those data"""
        pattern_event_names = [event.name for event in pattern.positive_structure.get_args()]
        pattern_event_types = [event.type for event in pattern.positive_structure.get_args()]
        pattern_event_indexes = [pattern.get_index_by_event_name(name) for name in pattern_event_names]
        wanted_event_index = node.event_index
        index_in_list = pattern_event_indexes.index(wanted_event_index)
        return wanted_event_index, pattern_event_types[index_in_list], pattern_event_names[index_in_list]

    @staticmethod
    def build_pattern_from_plan_node(node: TreePlanNode, pattern1: Pattern, leaves_dict, first_time=False):
        """Disciption: given an internal tree plan node we build recursively the pattern that describe the tree plan
            with root=node and this will be the sub pattern of the big pattern"""
        # first time we use Pattern construct unlike applying operator constructors in the next iterations
        if first_time:
            condition = UnifiedTreeBuilder.get_condition_from_pattern_in_one_sub_tree(node, pattern1, leaves_dict)
            pattern = Pattern(algoA.build_pattern_from_plan_node(node, pattern1, leaves_dict), condition,
                              pattern1.window, pattern1.consumption_policy, pattern1.id)
            pattern.set_statistics(pattern1.statistics_type, pattern1.statistics)
            return pattern

        node_type = type(node)
        if issubclass(node_type, TreePlanLeafNode):
            leaves_in_plan_node_1 = node.get_leaves()
            assert len(leaves_in_plan_node_1) == 1

            index, event_type, event_name = algoA.get_event_args(node, pattern1)
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
        """Description: we pass two patterns,and return a list with all
        the subpattern that are sharable between both pattern"""
        tree1_subtrees = algoA.get_all_nodes(tree_plan1.root)
        tree2_subtrees = algoA.get_all_nodes(tree_plan2.root)
        sharable = []
        pattern_to_tree_plan_map = {pattern1: tree_plan1, pattern2: tree_plan2}
        leaves_dict = UnifiedTreeBuilder.get_pattern_leaves_dict(pattern_to_tree_plan_map)

        for node1, node2 in itertools.product(tree1_subtrees, tree2_subtrees):
            if UnifiedTreeBuilder.is_equivalent(node1, pattern1, node2, pattern2, leaves_dict):
                leaves_in_plan_node_1 = node1.get_leaves()
                leaves_in_plan_node_2 = node2.get_leaves()
                if leaves_in_plan_node_1 is None or leaves_in_plan_node_2 is None:
                    return None, None
                pattern1_leaves, pattern1_events = list(zip(*list(leaves_dict.get(pattern1).items())))
                pattern2_leaves, pattern2_events = list(zip(*list(leaves_dict.get(pattern2).items())))

                event_indexes1 = list(map(lambda e: e.event_index, leaves_in_plan_node_1))
                plan_node_1_events = list(
                    filter(lambda i: pattern1_leaves[i].event_index in event_indexes1, range(len(pattern1_leaves))))

                event_indexes2 = list(map(lambda e: e.event_index, leaves_in_plan_node_2))
                plan_node_2_events = list(
                    filter(lambda i: pattern2_leaves[i].event_index in event_indexes2, range(len(pattern2_leaves))))

                names1 = {pattern1_events[event_index].name for event_index in plan_node_1_events}
                names2 = {pattern2_events[event_index].name for event_index in plan_node_2_events}
                sharable_sub_pattern = algoA.build_pattern_from_plan_node(node1, pattern1, leaves_dict, first_time=True)
                # time window is set to be the min between them by defintion
                sharable_sub_pattern.set_time_window(min(pattern1.window,
                                                         pattern2.window))
                sharable_sub_pattern_data = (sharable_sub_pattern, event_indexes1, names1, event_indexes2, names2)
                sharable.append(sharable_sub_pattern_data)
        return sharable

    @staticmethod
    def get_shareable_pairs(patterns: List[Pattern] or TreePlan):
        """we build a [n][n] matrix we store [i][j] a list with all sharable
            sub patterns between pattern i and pattern j"""
        number_of_patterns = len(patterns)
        shape = (number_of_patterns, number_of_patterns)
        shareable_pairs_array = np.empty(shape=shape, dtype=list)

        for i, patterni in enumerate(patterns):
            for j, patternj in enumerate(patterns):
                if j == i:
                    continue
                if j < i:  # because shareable_pairs_array[i][j] = shareable_pairs_array[j][i]
                    shareable_pairs_array[i][j] = shareable_pairs_array[j][i]
                    continue
                unified_builder = UnifiedTreeBuilder(tree_plan_order_approach=TreePlanBuilderOrder.LEFT_TREE)
                """
                unified_builder = UnifiedTreeBuilder(tree_plan_order_approach=TreePlanBuilderOrder.LEFT_TREE)
                tree_plan_i = unified_builder.build_tree_plan(patterni)
                tree_plan_j = unified_builder.build_tree_plan(patternj)
                """
                pattern_to_tree_plan_map_ordered = unified_builder.build_ordered_tree_plans([patterni,patternj])
                tree_plan1 = pattern_to_tree_plan_map_ordered[patterni]
                tree_plan2 = pattern_to_tree_plan_map_ordered[patternj]
                sharable_sub_patterns = algoA.get_all_sharable_sub_patterns(tree_plan1, patterni, tree_plan2, patternj)
                shareable_pairs_array[i][j] = sharable_sub_patterns
        return shareable_pairs_array

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

        pattern_to_tree_plan_map_copy = pattern_to_tree_plan_map.copy()
        sub_pattern_shareable_array_copy = deepcopy(sub_pattern_shareable_array)
        for p, tree in pattern_to_tree_plan_map_copy.items():
            new_tree = TreePlan(root=tree.root.get_node_copy())
            pattern_to_tree_plan_map_copy[p] = new_tree
        patterns_list = list(enumerate(pattern_to_tree_plan_map_copy.keys()))
        random_patterns_pair = random.sample(patterns_list, k=2)
        shareable_pairs_i_j = []
        i, j, patterni, patternj = 0, 0, None, None
        while len(shareable_pairs_i_j) == 0:
            i, patterni = random_patterns_pair[0]
            j, patternj = random_patterns_pair[1]
            if i > j:  # swap such that i < j
                tmp = random_patterns_pair[0]
                i, patterni = random_patterns_pair[1]
                j, patternj = tmp

            shareable_pairs_i_j = sub_pattern_shareable_array_copy[i, j]
            random_patterns_pair = random.sample(patterns_list, k=2)

        if len(shareable_pairs_i_j) == 0:
            """
            no share exist
            """
            return pattern_to_tree_plan_map_copy, sub_pattern_shareable_array_copy
        assert len(shareable_pairs_i_j) > 0

        random_idx = random.choice(range(len(shareable_pairs_i_j)))  # TODO
        # random_idx = 0
        assert random_idx < len(shareable_pairs_i_j)

        sub_pattern, event_indexes1, names1, event_indexes2, names2 = shareable_pairs_i_j[random_idx]

        # shareable_pairs_i_j[0][0].full_structure == patternj.full_structure, shareable_pairs_i_j[0][0].condition == patternj.condition

        alg = algoA()
        sub_pattern_data_1 = (sub_pattern, event_indexes1, names1)
        sub_pattern_data_2 = (sub_pattern, event_indexes2, names2)

        new_tree_plan_i = alg._create_tree_topology_shared_subpattern(patterni, sub_pattern_data_1)
        new_tree_plan_j = alg._create_tree_topology_shared_subpattern(patternj, sub_pattern_data_2)

        # merge
        if sub_pattern.is_equivalent(patterni) and sub_pattern.is_equivalent(patternj):
            new_tree_plan_i.root = new_tree_plan_j.root
        elif sub_pattern.is_equivalent(patterni):
            new_tree_plan_j.root.left_child = new_tree_plan_i.root
        elif sub_pattern.is_equivalent(patternj):
            new_tree_plan_i.root.left_child = new_tree_plan_j.root
        else:
            new_tree_plan_i.root.left_child = new_tree_plan_j.root.left_child

        pattern_to_tree_plan_map_copy[patterni] = new_tree_plan_i
        pattern_to_tree_plan_map_copy[patternj] = new_tree_plan_j

        del sub_pattern_shareable_array_copy[i, j][random_idx]
        # del sub_pattern_shareable_array[j, i][random_idx]

        return pattern_to_tree_plan_map_copy, sub_pattern_shareable_array_copy

    @staticmethod
    def Nvertex_neighborhood(pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan,
                             sub_pattern_shareable_array: np.array, k=DefaultConfig.SELECT_NEIGHBOR_PATTERNS):
        # k = max(2, sub_pattern_shareable_array.shape[0] // 4)
        """
        the Nedge neighborhood function as explained in the article Section 4.2

        :param pattern_to_tree_plan_map : dict that maps every pattern in the MPT to his TreePlan
        :param sub_pattern_shareable_array : a [n][n] matrix (n is number of patterns in MPT ) ,
                   where sub_pattern_shareable_array[i][j] = the list of all equal  sub patterns between patterni and patternj
        :return: creates new tree plans where sub pattern of two patterns merged

        """
        if isinstance(pattern_to_tree_plan_map, TreePlan) or len(pattern_to_tree_plan_map) <= 1:
            return pattern_to_tree_plan_map
        pattern_to_tree_plan_map_copy = pattern_to_tree_plan_map.copy()
        sub_pattern_shareable_array_copy = deepcopy(sub_pattern_shareable_array)
        patterns_list = list(enumerate(pattern_to_tree_plan_map_copy.keys()))
        for p, tree in pattern_to_tree_plan_map_copy.items():
            new_tree = TreePlan(root=tree.root.get_node_copy())
            pattern_to_tree_plan_map_copy[p] = new_tree
        # select random two patterns to select a random sub pattern that is sharable between the two
        random_patterns_pair = random.sample(patterns_list, k=2)
        shareable_pairs_i_j = []
        i, j, patterni, patternj = 0, 0, None, None
        while len(shareable_pairs_i_j) == 0:
            i, patterni = random_patterns_pair[0]
            j, patternj = random_patterns_pair[1]
            if i > j:  # swap such that i < j
                tmp = random_patterns_pair[0]
                i, patterni = random_patterns_pair[1]
                j, patternj = tmp
            shareable_pairs_i_j = sub_pattern_shareable_array_copy[i, j]
            random_patterns_pair = random.sample(patterns_list, k=2)

        if len(shareable_pairs_i_j) == 0:
            """ no share exist """
            return pattern_to_tree_plan_map_copy, sub_pattern_shareable_array_copy
        assert len(shareable_pairs_i_j) > 0
        # here we select the random sub pattern to share
        random_idx = random.choices(range(len(shareable_pairs_i_j)), k=1)[0]
        assert random_idx < len(shareable_pairs_i_j)
        sub_pattern, event_indexes1, names1, event_indexes2, names2 = shareable_pairs_i_j[random_idx]
        # now we must share this sub_ pattern with all patterns containing this sub_pattern
        all_pattern_indexes_contains_sub_pattern = [i]
        for idx in range(len(patterns_list)):
            if idx == i:
                continue
            patterns_i_idx_data = sub_pattern_shareable_array_copy[i, idx]
            sub_patterns_i_idx = np.array(patterns_i_idx_data)
            if len(sub_patterns_i_idx) > 0:  # get patterns from patterns_i_j_data (first column)
                sub_patterns_i_idx = sub_patterns_i_idx[:, 0]
            # sub_patterns_i_j = [patterns_i_j_data[k][0] for k in range(len(patterns_i_j_data))]
            is_j_contain_sub_pattern = len(
                list(filter(lambda pattern: pattern.is_equivalent(sub_pattern), sub_patterns_i_idx))) > 0
            if is_j_contain_sub_pattern:
                all_pattern_indexes_contains_sub_pattern.append(idx)
        all_pattern_indexes_contains_sub_pattern = np.array(sorted(all_pattern_indexes_contains_sub_pattern))
        assert len(all_pattern_indexes_contains_sub_pattern) >= 2
        assert i in all_pattern_indexes_contains_sub_pattern
        assert j in all_pattern_indexes_contains_sub_pattern
        max_sharing = min(k, len(all_pattern_indexes_contains_sub_pattern))
        alg = algoA()
        sub_pattern_data_1 = (sub_pattern, event_indexes1, names1)
        new_tree_plan1 = alg._create_tree_topology_shared_subpattern(patterni, sub_pattern_data_1)
        pattern_to_tree_plan_map_copy[patterni] = new_tree_plan1

        # patterns_contains_sub_pattern = sub_pattern_shareable_array_copy[i][all_pattern_indexes_contains_sub_pattern]

        # TODO : change the sharing from sharing all to share only max_sharing
        for p_idx in all_pattern_indexes_contains_sub_pattern[:max_sharing]:
            if p_idx == i:
                continue
            sub_pattern_index = -1
            shareable_pairs_i_p_idx = sub_pattern_shareable_array_copy[i, p_idx]
            # find sub pattern index at shareable[i][p_idx]
            pattern = patterns_list[p_idx][1]
            # find sub pattern index at shareable[i][p_idx]
            for j, shared_sub_pattern in enumerate(shareable_pairs_i_p_idx):
                if shared_sub_pattern[0].is_equivalent(sub_pattern):
                    sub_pattern_index = j
                    break
            if sub_pattern_index == -1:
                raise Exception("Error , must find the sub pattern index")
            sub_pattern, event_indexes1, names1, curr_event_indexes, curr_names = shareable_pairs_i_p_idx[
                sub_pattern_index]
            curr_sub_pattern_data = (sub_pattern, curr_event_indexes, curr_names)
            curr_new_plan = alg._create_tree_topology_shared_subpattern(pattern, curr_sub_pattern_data)
            # do the sharing process
            if sub_pattern.is_equivalent(patterni) and sub_pattern.is_equivalent(patternj):
                curr_new_plan.root = new_tree_plan1.root
            elif sub_pattern.is_equivalent(patterni):
                curr_new_plan.root.left_child = new_tree_plan1.root
            elif sub_pattern.is_equivalent(patternj):
                curr_new_plan.root.left_child = new_tree_plan1.root
            else:
                curr_new_plan.root.left_child = new_tree_plan1.root.left_child

            pattern_to_tree_plan_map_copy[pattern] = curr_new_plan
            del sub_pattern_shareable_array_copy[i, p_idx][sub_pattern_index]
        return pattern_to_tree_plan_map_copy, sub_pattern_shareable_array_copy


""" ======================================== Tests for all functions in MPT_edge_neighborhood ====================="""


def single_pattern_cost(pattern: Pattern, tree_plan_root: TreePlanNode, cost_model: TreeCostModel):
    try:
        cost = cost_model.get_plan_cost(pattern, tree_plan_root)
        return cost
    except:
        return 0  # TODO??


def get_duplicated_cost(pattern_i, tree_plan_i_root, pattern_j, tree_plan_j_root, cost_model: TreeCostModel):
    node1, node2 = None, None
    if tree_plan_i_root == tree_plan_j_root:
        node1 = tree_plan_i_root
        node2 = tree_plan_j_root
    elif tree_plan_i_root.left_child == tree_plan_j_root:
        node1 = tree_plan_i_root.left_child
        node2 = tree_plan_j_root
    elif tree_plan_i_root == tree_plan_j_root.left_child:
        node1 = tree_plan_i_root
        node2 = tree_plan_j_root.left_child
    elif tree_plan_i_root.left_child == tree_plan_j_root.left_child:
        node1 = tree_plan_i_root.left_child
        node2 = tree_plan_j_root.left_child

    if node1 is None or node2 is None:
        return 0

    cost_i = single_pattern_cost(pattern_i, tree_plan_i_root, cost_model)
    cost_j = single_pattern_cost(pattern_j, tree_plan_j_root, cost_model)
    duplicated_cost = min(cost_i, cost_j)
    return duplicated_cost


def tree_plan_cost_function(state: Tuple[Dict[Pattern, TreePlan], np.array]):
    pattern_to_tree_plan_map, _ = state
    cost_model = TreeCostModelFactory.create_cost_model()
    patterns = list(pattern_to_tree_plan_map.keys())

    tree_plan_total_cost = sum(
        [single_pattern_cost(p, tree_plan.root, cost_model) for p, tree_plan in pattern_to_tree_plan_map.items()])
    for i, j in itertools.product(range(len(patterns)), range(len(patterns))):
        if i >= j:
            continue
        pattern_i = patterns[i]
        pattern_j = patterns[j]
        tree_plan_i_root = pattern_to_tree_plan_map[pattern_i].root
        tree_plan_j_root = pattern_to_tree_plan_map[pattern_j].root
        duplicated_cost = get_duplicated_cost(pattern_i, tree_plan_i_root, pattern_j, tree_plan_j_root, cost_model)
        tree_plan_total_cost -= duplicated_cost

    return tree_plan_total_cost


def patterns_initialize_function(patterns: List[Pattern]):
    alg = algoA()
    pattern_to_tree_plan_map = {p: alg._create_tree_topology(p) for p in patterns}
    shareable_pairs = algoA.get_shareable_pairs(patterns)
    return pattern_to_tree_plan_map, shareable_pairs


def tree_plan_neighbour(state: Tuple[Dict[Pattern, TreePlan], np.array]):
    """Move a little bit x, from the left or the right."""
    pattern_to_tree_plan_map, shareable_pairs = state
    alg = algoA()
    neighbour = alg.Nedge_neighborhood(pattern_to_tree_plan_map, shareable_pairs)
    return neighbour


iter_num = 1


def tree_plan_state_get_summary(state: Tuple[Dict[Pattern, TreePlan], np.array]):
    _, shareable_pairs = state
    count_common_pairs = lambda lst: len(lst) if lst is not None else 0
    global iter_num
    iter_num += 1
    # dividing by 2 cause the shareable_pairs is a symmetric matrix
    return "iter " + str(iter_num) + " common pairs size = " + str(
        np.sum([count_common_pairs(lst) for lst in shareable_pairs.reshape(-1)]) // 2)


def tree_plan_equal(state1: Tuple[Dict[Pattern, TreePlan], np.array],
                    state2: Tuple[Dict[Pattern, TreePlan], np.array]):
    leaves_dict = {}

    patterns = list(state1[0].keys())

    leaves_dict = UnifiedTreeBuilder.get_pattern_leaves_dict(state1[0])

    tree_plans1 = list([tree_plan for _, tree_plan in state1[0].items()])
    tree_plans2 = list([tree_plan for _, tree_plan in state2[0].items()])

    for idx, pattern in enumerate(patterns):
        tree_plans1_root = tree_plans1[idx].root
        tree_plans2_root = tree_plans2[idx].root
        if not UnifiedTreeBuilder.is_equivalent(tree_plans1_root, pattern, tree_plans2_root, pattern, leaves_dict):
            return False
    return True


# =======================================================================================

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


def create_topology_const_sub_pattern_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("GOOG", "d")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
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

    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895]
    pattern2.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    algoA_instance = algoA()

    new_plan = algoA_instance._create_tree_topology_shared_subpattern(pattern1, pattern2)
    print('Ok')


def create_topology_sub_pattern_eq_pattern_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("GOOG", "d")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
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
    pattern2.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))

    patterns = [pattern1, pattern2]
    eval_mechanism_params = TreeBasedEvaluationMechanismParameters()
    tree_plan_builder = TreePlanBuilderFactory.create_tree_plan_builder(eval_mechanism_params.tree_plan_params)
    tree_plan = tree_plan_builder.build_tree_plan(pattern1)
    pattern_to_tree_plan_map = {p: tree_plan_builder.build_tree_plan(p) for p in patterns}

    algoA_instance = algoA()
    pattern2_data = (pattern2, range(4), {'a', 'b', 'c', 'd'})
    new_plan = algoA_instance._create_tree_topology_shared_subpattern(pattern1, pattern2_data)
    print('Ok')


def Nedge_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("GOOG", "d")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("GOOG", "d")),
        AndCondition(),
        timedelta(minutes=5)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern1.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))

    selectivityMatrix = [[1.0, 0.15989723367389616], [1.0, 1.0]]
    arrivalRates = [0.013917884481558803, 0.012421711899791231]
    pattern2.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    patterns = [pattern1, pattern2]

    state = patterns_initialize_function(patterns)
    pattern_to_tree_plan_map, shareable_pairs = state
    alg = algoA()
    alg.Nedge_neighborhood(pattern_to_tree_plan_map, shareable_pairs)
    print('Ok')


def annealing_basic_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("GOOG", "d")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("GOOG", "d")),
        AndCondition(),
        timedelta(minutes=5)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern1.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))

    selectivityMatrix = [[1.0, 0.15989723367389616], [1.0, 1.0]]
    arrivalRates = [0.013917884481558803, 0.012421711899791231]
    pattern2.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    patterns = [pattern1, pattern2]
    state, c = tree_plan_visualize_annealing(patterns=patterns,
                                             initialize_function=patterns_initialize_function,
                                             state_repr_function=tree_plan_state_get_summary,
                                             state_equal_function=tree_plan_equal,
                                             cost_function=tree_plan_cost_function,
                                             neighbour_function=tree_plan_neighbour)
    pattern_to_tree_plan_map, shareable_pairs = state


def annealing_med_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("SALEH", "d")),
        AndCondition(
            GreaterThanCondition(Variable("b", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "e"), PrimitiveEventStructure("SALEH", "f")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    pattern3 = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("b", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern1.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern2.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))

    selectivityMatrix = [[1.0, 0.9457796098355941], [0.9457796098355941, 0.15989723367389616]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895]

    pattern3.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    patterns = [pattern1, pattern2, pattern3]
    state, c = tree_plan_visualize_annealing(patterns=patterns,
                                             initialize_function=patterns_initialize_function,
                                             state_repr_function=tree_plan_state_get_summary,
                                             state_equal_function=tree_plan_equal,
                                             cost_function=tree_plan_cost_function,
                                             neighbour_function=tree_plan_neighbour)
    pattern_to_tree_plan_map, shareable_pairs = state
    return pattern_to_tree_plan_map


def basic_Nvertex_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("SALEH", "d")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("SALEH", "d")),
        AndCondition(),
        timedelta(minutes=5)
    )
    pattern3 = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("TONY", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("SALEH", "d")),
        AndCondition(),
        timedelta(minutes=5)

    )

    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern1.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern3.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    selectivityMatrix = [[1.0, 0.15989723367389616], [1.0, 1.0]]
    arrivalRates = [0.013917884481558803, 0.012421711899791231]
    pattern2.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    patterns = [pattern1, pattern2, pattern3]
    state = patterns_initialize_function(patterns)
    pattern_to_tree_plan_map, shareable_pairs = state
    alg = algoA()
    alg.Nvertex_neighborhood(pattern_to_tree_plan_map, shareable_pairs, 3)
    print('Ok')


def advanced_Nvertex_no_conditions_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("SALEH", "d")),
        AndCondition(),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"),
                    PrimitiveEventStructure("TONY", "b"),
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("SALEH", "d")),
        AndCondition(),
        timedelta(minutes=5)

    )
    pattern3 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("SALEH", "d")),
        AndCondition(),
        timedelta(minutes=5)
    )
    pattern4 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(),
        timedelta(minutes=5)
    )

    pattern5 = Pattern(
        SeqOperator(PrimitiveEventStructure("FB", "e"),
                    PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b")),
        AndCondition(),
        timedelta(minutes=5)
    )

    pattern6 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("LI", "c")),
        AndCondition(),
        timedelta(minutes=2)
    )
    pattern7 = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"),
                    PrimitiveEventStructure("TONY", "b"),
                    PrimitiveEventStructure("FB", "e")),
        AndCondition(),
        timedelta(minutes=5)
    )

    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0],
                         [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864],
                         [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern1.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern2.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    selectivityMatrix = [[1.0, 0.15989723367389616],
                         [1.0, 1.0]]
    arrivalRates = [0.013917884481558803, 0.012421711899791231]
    pattern3.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0],
                         [0.9457796098355941, 1.0, 0.15989723367389616],
                         [1.0, 0.15989723367389616, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803]
    pattern4.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern5.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern6.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern7.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))

    patterns = [pattern1, pattern2, pattern3, pattern4, pattern5, pattern6, pattern7]
    state = patterns_initialize_function(patterns)
    pattern_to_tree_plan_map, shareable_pairs = state
    alg = algoA()
    alg.Nvertex_neighborhood(pattern_to_tree_plan_map, shareable_pairs, 7)
    print('Ok')


def advanced_Nvertex_test():
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("SALEH", "d")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("TONY", "b"),
                    PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("SALEH", "d")),
        AndCondition(),
        timedelta(minutes=5)

    )
    pattern3 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "c"), PrimitiveEventStructure("SALEH", "d")),
        AndCondition(),
        timedelta(minutes=5)
    )
    pattern4 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("c", lambda x: x["Peak Price"]), 500)
        ),
        timedelta(minutes=5)
    )

    pattern5 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("c", lambda x: x["Peak Price"]), 530)
        ),
        timedelta(minutes=3)
    )

    pattern6 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("FB", "e")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("e", lambda x: x["Peak Price"]), 520)
        ),
        timedelta(minutes=5)
    )

    pattern7 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("LI", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("c", lambda x: x["Peak Price"]), 100)
        ),
        timedelta(minutes=2)
    )
    pattern8 = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("TONY", "b"),
                    PrimitiveEventStructure("FB", "e")),
        AndCondition(),
        timedelta(minutes=5)
    )
    pattern9 = Pattern(
        SeqOperator(PrimitiveEventStructure("FB", "e"),
                    PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("TONY", "b")),
        AndCondition(),
        timedelta(minutes=5)
    )

    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0],
                         [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864],
                         [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern1.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern2.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    selectivityMatrix = [[1.0, 0.15989723367389616],
                         [1.0, 1.0]]
    arrivalRates = [0.013917884481558803, 0.012421711899791231]
    pattern3.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0],
                         [0.9457796098355941, 1.0, 0.15989723367389616],
                         [1.0, 0.15989723367389616, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803]
    pattern4.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern5.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern6.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern7.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern8.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    pattern9.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    patterns = [pattern1, pattern2, pattern3, pattern4, pattern5, pattern6, pattern7]
    state = patterns_initialize_function(patterns)
    pattern_to_tree_plan_map, shareable_pairs = state
    alg = algoA()
    alg.Nvertex_neighborhood(pattern_to_tree_plan_map, shareable_pairs, 7)
    print('Ok')


if __name__ == '__main__':
    # pattern_to_tree_plan_map = annealing_med_test()
    #
    # eval_mechanism_params = TreeBasedEvaluationMechanismParameters()
    # unified_tree = TreeBasedEvaluationMechanism(pattern_to_tree_plan_map, eval_mechanism_params.storage_params,
    #                                             eval_mechanism_params.multi_pattern_eval_params)
    # unified_tree.visualize(title="SMT unified Tree")
    advanced_Nvertex_test()
