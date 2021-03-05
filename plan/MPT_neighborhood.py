import itertools
import random
from copy import deepcopy
from typing import List, Dict, Tuple

from SimulatedAnnealing import SimulatedAnnealing
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, OrOperator, AndOperator, KleeneClosureOperator, NegationOperator, \
    PrimitiveEventStructure
from misc import DefaultConfig
from misc.StatisticsTypes import StatisticsTypes
from misc.Utils import get_all_disjoint_sets
from plan.TopologyChangeSharingTreeBuilder import TopologyChangeSharingTreeBuilder
from plan.TreeCostModel import TreeCostModelFactory, TreeCostModel
from plan.TreePlan import *
from plan.TreePlanBuilder import TreePlanBuilder
from plan.UnifiedTreeBuilder import UnifiedTreeBuilder
from plan.multi.MultiPatternUnifiedTreeLocalSearchApproaches import MultiPatternUnifiedTreeLocalSearchApproaches


class algoA(TreePlanBuilder):
    def create_tree_topology_aux(self, pattern: Pattern, items, args_num):
        # this function is used as an auxiliary functions in 3 different functions:
        # create_tree_topology , _create_topology_with_const_sub_order and _create_pattern_topology
        """
        @params:
            TreePlanBuilder instance
        @returns:
            creates and returns a treePlan for  the given pattern
            we do that using dynamic programming , in each iteration we calculate the sub plan cost
            and pick the the one with the cheapest cost , the best sub plan is what we consider to
            build from for the next iterations , note that cost(plan) is calculated by the pattern statistics ,
            if no statistics passed then there is no meaning to the cheapest plan ,
            we simply build any plan and return it
        """
        sub_trees = {frozenset({i}): (TreePlanLeafNode(i),
                                      self._get_plan_cost(pattern, TreePlanLeafNode(i)),
                                      items.difference({i}))
                     for i in items}
        # for each subset of size i, find optimal topology for these subsets according to size (i-1) subsets.
        for i in range(2, args_num + 1):
            for tSubset in itertools.combinations(items, i):
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
        # return the best topology (index 0 at tuple) for items - the set of all arguments.
        return sub_trees[items][0]

    def create_tree_topology(self, pattern: Pattern):
        """
        @params :
            pattern
        @returns:  treePlan topolgy for the given pattern
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
        return TreePlan(self.create_tree_topology_aux(pattern, items, args_num))

    def _create_topology_with_const_sub_order(self, pattern: Pattern, const_sub_ord: list):
        """
        @params:
            1) pattern : to build the topology for.
            2) const_sub_ord: list of events indexes that shouldn't be considered in creating the pattern topology
        @returns: the same as create_tree_topology , only that this time we build the plan for the
            complementary pattern, the complementary pattern is defined by the pattern events indexes minus
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
        return self.create_tree_topology_aux(pattern, items, args_num)

    def _create_pattern_topology(self, sub_pattern_data: Tuple):
        """
        @params:
            sub_pattern_data: a tuple of 3 :
             * first entry is the sub_pattern itself
             * second entry is a list of all event indexes of this pattern
             * third entry is a list of all event names of this pattern
        Returns :
            return a tree topology for the sub_pattern , we pass  the sub_pattern_data
            to build the sub_pattern we custom build where we choose what the events are in the sub tree
            because we want those same indexes of the big pattern that includes the sub pattern
            and not some the automatically generated indexes
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
        return self.create_tree_topology_aux(pattern, items, args_num)

    def _create_tree_topology_shared_subpattern(self, pattern: Pattern, sub_pattern_data: Tuple):
        """
        @params:
            pattern: to build a tree topolgy for
            sub_pattern_data: look at _create_pattern_topology for this parameter documentation
        @returns:
            this function builds and returns the best tree topology with one restriction in mind:
            the pattern has to starts with the subpattern order
            ,remained pattern we build according to best order possible i.e. the order with the minimal cost"""
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
        """
        params:
            TreePlanNode : the node we need to traverse it and return all nodes in his subtree
        @returns:
            all plan nodes in the sub tree of the  plan_node"""
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
        """
        param:
            node: a plan node of the type TreePlanLeafNode
            pattern: the Pattern that contains the data of the above node
        @returns : the data of this leaf node :event index , type and name, all this data is extracted from the pattern
        """
        pattern_event_names = [event.name for event in pattern.positive_structure.get_args()]
        pattern_event_types = [event.type for event in pattern.positive_structure.get_args()]
        pattern_event_indexes = [pattern.get_index_by_event_name(name) for name in pattern_event_names]
        wanted_event_index = node.event_index
        index_in_list = pattern_event_indexes.index(wanted_event_index)
        return wanted_event_index, pattern_event_types[index_in_list], pattern_event_names[index_in_list]

    @staticmethod
    def build_pattern_from_plan_node(node: TreePlanNode, pattern: Pattern, leaves_dict, first_call=False):
        """
            params:
                node: internal tree plan node
                pattern: pattern that describe the tree plan with root=node
                leaves_dict: we use the Pattern constructor in this function that needs this argument
                first_call: since we build the Pattern recursively we need to identify the first call of this function
                            because the first build we use the Pattern constructor and the rest of calls we build
                            according to the type of currnode
            @returns:
                A compatible for the given node subtree
            """
        # first time we use Pattern construct unlike applying operator constructors in the next iterations
        if first_call:
            condition = UnifiedTreeBuilder.get_condition_from_pattern_in_one_sub_tree(node, pattern, leaves_dict)
            pattern = Pattern(algoA.build_pattern_from_plan_node(node, pattern, leaves_dict), condition,
                              pattern.window, pattern.consumption_policy, pattern.id)
            pattern.set_statistics(pattern.statistics_type, pattern.statistics)
            return pattern

        node_type = type(node)
        if issubclass(node_type, TreePlanLeafNode):
            leaves_in_plan_node_1 = node.get_leaves()
            assert len(leaves_in_plan_node_1) == 1

            index, event_type, event_name = algoA.get_event_args(node, pattern)
            return PrimitiveEventStructure(event_type, event_name)

        elif issubclass(node_type, TreePlanInternalNode):  # internal node
            node_operator: OperatorTypes = node.get_operator()
            if node_operator == OperatorTypes.SEQ:
                return SeqOperator(algoA.build_pattern_from_plan_node(node.left_child, pattern, leaves_dict),
                                   algoA.build_pattern_from_plan_node(node.right_child, pattern, leaves_dict))
            elif node_operator == OperatorTypes.OR:
                return OrOperator(algoA.build_pattern_from_plan_node(node.left_child, pattern, leaves_dict),
                                  algoA.build_pattern_from_plan_node(node.right_child, pattern, leaves_dict))
            elif node_operator == OperatorTypes.AND:
                return AndOperator(algoA.build_pattern_from_plan_node(node.left_child, pattern, leaves_dict),
                                   algoA.build_pattern_from_plan_node(node.right_child, pattern, leaves_dict))
            elif node_operator == OperatorTypes.KC:
                return KleeneClosureOperator(
                    algoA.build_pattern_from_plan_node(node.child, pattern, leaves_dict))
            elif node_operator == OperatorTypes.NSEQ:
                return NegationOperator(
                    SeqOperator(algoA.build_pattern_from_plan_node(node.left_child, pattern, leaves_dict),
                                algoA.build_pattern_from_plan_node(node.right_child, pattern, leaves_dict)))
            elif node_operator == OperatorTypes.NAND:
                return NegationOperator(
                    AndOperator(algoA.build_pattern_from_plan_node(node.left_child, pattern, leaves_dict),
                                algoA.build_pattern_from_plan_node(node.right_child, pattern, leaves_dict)))
            else:
                raise NotImplementedError

    @staticmethod
    def get_all_sharable_sub_patterns(first_tree_plan: TreePlan, first_pattern: Pattern, second_tree_plan: TreePlan,
                                      second_pattern: Pattern):
        """
        @params:
            two plans with their compatible patterns
        @returns:
            A list with all the subpattern that are sharable between the given patterns"""
        fisrt_tree_subtrees = algoA.get_all_nodes(first_tree_plan.root)
        second_tree_subtrees = algoA.get_all_nodes(second_tree_plan.root)
        sharable = []
        pattern_to_tree_plan_map = {first_pattern: first_tree_plan, second_pattern: second_tree_plan}
        leaves_dict = UnifiedTreeBuilder.get_pattern_leaves_dict(pattern_to_tree_plan_map)

        for node1, node2 in itertools.product(fisrt_tree_subtrees, second_tree_subtrees):
            if TreePlanBuilder.is_equivalent(node1, first_pattern, node2, second_pattern, leaves_dict):
                first_names, _, fist_event_indexes = TreePlanBuilder.extract_pattern_condition(node1, first_pattern,
                                                                                               leaves_dict)
                second_names, _, second_event_indexes = TreePlanBuilder.extract_pattern_condition(node2, second_pattern,
                                                                                                  leaves_dict)
                sharable_sub_pattern = algoA.build_pattern_from_plan_node(node1, first_pattern, leaves_dict,
                                                                          first_time=True)
                # time window is set to be the min between them by defintion
                sharable_sub_pattern.set_time_window(min(first_pattern.window,
                                                         second_pattern.window))
                sharable_sub_pattern_data = (
                sharable_sub_pattern, fist_event_indexes, first_names, second_event_indexes, second_names)
                sharable.append(sharable_sub_pattern_data)
        return sharable

    @staticmethod
    def get_shareable_pairs(patterns: List[Pattern] or Pattern):
        """
            @params:
                patterns: a list of all patterns or one pattern
            @returns:
                return [n][n] matrix, we store in [i][j] a list with all sharable
                sub patterns between pattern i and pattern j"""
        number_of_patterns = len(patterns)
        # shape = (number_of_patterns, number_of_patterns)
        # shareable_pairs_array = np.empty(shape=shape, dtype=list)
        shareable_pairs_array = [[None for _ in range(number_of_patterns)] for _ in range(number_of_patterns)]
        for i, patterni in enumerate(patterns):
            for j, patternj in enumerate(patterns):
                if j == i:
                    continue
                if j < i:  # because shareable_pairs_array[i][j] = shareable_pairs_array[j][i]
                    shareable_pairs_array[i][j] = shareable_pairs_array[j][i]
                    continue
                unified_builder = TopologyChangeSharingTreeBuilder()
                """
                unified_builder = UnifiedTreeBuilder(tree_plan_order_approach=TreePlanBuilderOrder.LEFT_TREE)
                tree_plan_i = unified_builder.build_tree_plan(patterni)
                tree_plan_j = unified_builder.build_tree_plan(patternj)
                """
                pattern_to_tree_plan_map_ordered = unified_builder.build_ordered_tree_plans([patterni, patternj])
                tree_plan1 = pattern_to_tree_plan_map_ordered[patterni]
                tree_plan2 = pattern_to_tree_plan_map_ordered[patternj]
                sharable_sub_patterns = algoA.get_all_sharable_sub_patterns(tree_plan1, patterni, tree_plan2, patternj)
                shareable_pairs_array[i][j] = sharable_sub_patterns
        return shareable_pairs_array

    @staticmethod
    def get_random_patterns_with_their_common_sub_patterns(patterns_list, sub_pattern_shareable_array):
        """ select random two patterns and all the common sub patterns between them
         we do that to select later a random sub pattern that is sharable between them """
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
            shareable_pairs_i_j = sub_pattern_shareable_array[i][j]
            random_patterns_pair = random.sample(patterns_list, k=2)
        return i, patterni, j, patternj, shareable_pairs_i_j

    @staticmethod
    def share_tree_plans(sub_pattern, patterni, new_tree_plan_i, patternj, new_tree_plan_j):
        # merge
        if sub_pattern.is_equivalent(patterni) and sub_pattern.is_equivalent(patternj):
            new_tree_plan_i.root = new_tree_plan_j.root
        elif sub_pattern.is_equivalent(patterni):
            new_tree_plan_j.root.left_child = new_tree_plan_i.root
        elif sub_pattern.is_equivalent(patternj):
            new_tree_plan_i.root.left_child = new_tree_plan_j.root
        else:
            new_tree_plan_i.root.left_child = new_tree_plan_j.root.left_child

    @staticmethod
    def Nedge_neighborhood(pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan,
                           sub_pattern_shareable_array):
        """
        the Nedge neighborhood function as explained in the article Section 4.2
        :param pattern_to_tree_plan_map : dict that maps every pattern in the MPT to his TreePlan
        :param sub_pattern_shareable_array : a [n][n] matrix (n is number of patterns in MPT ) ,
                   where sub_pattern_shareable_array[i][j] = the list of all equal  sub patterns between patterni and patternj
        :return: creates new tree plans where sub pattern of two patterns merged
        """
        if isinstance(pattern_to_tree_plan_map, TreePlan) or len(pattern_to_tree_plan_map) <= 1:
            return pattern_to_tree_plan_map, sub_pattern_shareable_array
        pattern_to_tree_plan_map_copy = pattern_to_tree_plan_map.copy()
        for p, tree in pattern_to_tree_plan_map_copy.items():
            new_tree = TreePlan(root=tree.root.get_node_copy())
            pattern_to_tree_plan_map_copy[p] = new_tree
        patterns_list = list(enumerate(pattern_to_tree_plan_map_copy.keys()))
        sub_pattern_shareable_array_copy = deepcopy(sub_pattern_shareable_array)
        # select random two patterns and all the common sub patterns between them
        # we do that to select later a random sub pattern that is sharable between them
        i, patterni, j, patternj, shareable_pairs_i_j = algoA.get_random_patterns_with_their_common_sub_patterns(
            patterns_list, sub_pattern_shareable_array_copy)
        if len(shareable_pairs_i_j) == 0:  # no share exists
            return pattern_to_tree_plan_map_copy, sub_pattern_shareable_array_copy
        assert len(shareable_pairs_i_j) > 0
        random_idx = random.choice(range(len(shareable_pairs_i_j)))  # TODO
        # random_idx = 0
        assert random_idx < len(shareable_pairs_i_j)
        sub_pattern, event_indexes1, names1, event_indexes2, names2 = shareable_pairs_i_j[random_idx]
        alg = algoA()
        sub_pattern_data_1 = (sub_pattern, event_indexes1, names1)
        sub_pattern_data_2 = (sub_pattern, event_indexes2, names2)
        new_tree_plan_i = alg._create_tree_topology_shared_subpattern(patterni, sub_pattern_data_1)
        new_tree_plan_j = alg._create_tree_topology_shared_subpattern(patternj, sub_pattern_data_2)
        # merge
        algoA.share_tree_plans(sub_pattern, patterni, new_tree_plan_i, patternj, new_tree_plan_j)
        pattern_to_tree_plan_map_copy[patterni] = new_tree_plan_i
        pattern_to_tree_plan_map_copy[patternj] = new_tree_plan_j

        del sub_pattern_shareable_array_copy[i][j][random_idx]
        return pattern_to_tree_plan_map_copy, sub_pattern_shareable_array_copy

    @staticmethod
    def Nvertex_neighborhood(pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan,
                             sub_pattern_shareable_array, k=DefaultConfig.SELECT_NEIGHBOR_PATTERNS):
        """
        the Nedge neighborhood function as explained in the article Section 4.2

        @params
            pattern_to_tree_plan_map : dict that maps every pattern in the MPT to his TreePlan
            sub_pattern_shareable_array : a [n][n] matrix (n is number of patterns in MPT ) ,
                                            where sub_pattern_shareable_array[i][j] = the list of all equal
                                            sub patterns between patterni and patternj
        @return: creates new tree plans where sub pattern of two patterns merged
                    and returns the new created pattern_to_tree_plan_map ,
                    in addition we return the sub_pattern_shareable_array

        """
        if isinstance(pattern_to_tree_plan_map, TreePlan) or len(pattern_to_tree_plan_map) <= 1:
            return pattern_to_tree_plan_map, sub_pattern_shareable_array
        pattern_to_tree_plan_map_copy = pattern_to_tree_plan_map.copy()
        sub_pattern_shareable_array_copy = deepcopy(sub_pattern_shareable_array)
        patterns_list = list(enumerate(pattern_to_tree_plan_map_copy.keys()))
        for p, tree in pattern_to_tree_plan_map_copy.items():
            new_tree = TreePlan(root=tree.root.get_node_copy())
            pattern_to_tree_plan_map_copy[p] = new_tree
        # select random two patterns and all the common sub patterns between them
        # we do that to select later a random sub pattern that is sharable between them
        i, patterni, j, patternj, shareable_pairs_i_j = algoA.get_random_patterns_with_their_common_sub_patterns(
            patterns_list, sub_pattern_shareable_array_copy)
        if len(shareable_pairs_i_j) == 0:  # no share exists
            return pattern_to_tree_plan_map_copy, sub_pattern_shareable_array_copy
        # here we select the random sub pattern to share
        random_idx = random.choices(range(len(shareable_pairs_i_j)), k=1)[0]
        sub_pattern, event_indexes1, names1, event_indexes2, names2 = shareable_pairs_i_j[random_idx]
        # now we must share this sub_ pattern with all patterns containing this sub_pattern
        all_pattern_indexes_contains_sub_pattern = [i]
        for idx in range(len(patterns_list)):
            if idx == i:
                continue
            patterns_i_idx_data = sub_pattern_shareable_array_copy[i][idx]
            if len(patterns_i_idx_data) == 0:
                continue

            sub_patterns_i_idx = patterns_i_idx_data
            if len(sub_patterns_i_idx) > 0:  # get patterns from patterns_i_j_data (first column)

                sub_patterns_i_idx = [sub_data[0] for sub_data in sub_patterns_i_idx]
            is_j_contain_sub_pattern = len(
                list(filter(lambda pattern1: pattern1.is_equivalent(sub_pattern), sub_patterns_i_idx))) > 0
            if is_j_contain_sub_pattern:
                all_pattern_indexes_contains_sub_pattern.append(idx)
        all_pattern_indexes_contains_sub_pattern = sorted(all_pattern_indexes_contains_sub_pattern)
        max_sharing = min(k, len(all_pattern_indexes_contains_sub_pattern))
        alg = algoA()
        sub_pattern_data_1 = (sub_pattern, event_indexes1, names1)
        new_tree_plani = alg._create_tree_topology_shared_subpattern(patterni, sub_pattern_data_1)
        pattern_to_tree_plan_map_copy[patterni] = new_tree_plani
        for p_idx in all_pattern_indexes_contains_sub_pattern[:max_sharing]:
            if p_idx == i:
                continue
            sub_pattern_index = -1
            shareable_pairs_i_p_idx = sub_pattern_shareable_array_copy[i][p_idx]
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
            algoA.share_tree_plans(sub_pattern, patterni, new_tree_plani, pattern, curr_new_plan)
            pattern_to_tree_plan_map_copy[pattern] = curr_new_plan
            del sub_pattern_shareable_array_copy[i][p_idx][sub_pattern_index]
        return pattern_to_tree_plan_map_copy, sub_pattern_shareable_array_copy

    @staticmethod
    def construct_subtrees_local_search_tree_plan(pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan,
                                                  tree_plan_local_search_params: Tuple[
                                                      MultiPatternUnifiedTreeLocalSearchApproaches, int]):
        """
        @ params:
            1)pattern_to_tree_plan_map :dict between the Patter and his treePlan
            2)tree_plan_local_search_params: a tuple of 2:
                * first entry is the local_search_neighbor_approach either Nedge or Nvertex approach
                * second entry is the time limit for the local search to run
        @returns:
            the new pattern_to_tree_plan_map
            This method gets patterns, builds a single-pattern tree to each one of them,
            and merges equivalent subtrees from different trees using simulated annealing local search algorithm.
        """
        local_search_neighbor_approach, time_limit = tree_plan_local_search_params
        patterns = list(pattern_to_tree_plan_map.keys())
        neighbour_function = None
        if local_search_neighbor_approach == MultiPatternUnifiedTreeLocalSearchApproaches.EDGE_NEIGHBOR:
            neighbour_function = tree_plan_edge_neighbour
        elif local_search_neighbor_approach == MultiPatternUnifiedTreeLocalSearchApproaches.VERTEX_NEIGHBOR:
            neighbour_function = tree_plan_vertex_neighbour
        elif local_search_neighbor_approach == MultiPatternUnifiedTreeLocalSearchApproaches.VERTEX_NEIGHBOR:
            raise Exception("Unsupported local search successor function")
        simulated_annealing_instance = SimulatedAnnealing(patterns=patterns,
                                                          initialize_function=patterns_initialize_function,
                                                          cost_function=tree_plan_cost_function,
                                                          neighbour_function=neighbour_function,
                                                          state_equal_function=tree_plan_equal,
                                                          state_repr_function=tree_plan_state_get_summary,
                                                          time_limit=time_limit)

        state, c, states, costs = simulated_annealing_instance.timed_annealing()
        pattern_to_tree_plan_map, _ = state
        return pattern_to_tree_plan_map


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


def tree_plan_cost_function(state: Tuple[Dict[Pattern, TreePlan], List[List]]):
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
    pattern_to_tree_plan_map = {p: alg.create_tree_topology(p) for p in patterns}
    shareable_pairs = algoA.get_shareable_pairs(patterns)
    return pattern_to_tree_plan_map, shareable_pairs


def tree_plan_edge_neighbour(state: Tuple[Dict[Pattern, TreePlan], List[List]]):
    """Move a little bit x, from the left or the right."""
    pattern_to_tree_plan_map, shareable_pairs = state
    alg = algoA()
    count_common_pairs = lambda lst: len(lst) if lst is not None else 0
    count_common_pairs_lst = lambda lst_of_lists: sum([count_common_pairs(lst) for lst in lst_of_lists])
    if sum([count_common_pairs_lst(lst_of_lists) for lst_of_lists in shareable_pairs]) == 0:
        return state
    neighbour = alg.Nedge_neighborhood(pattern_to_tree_plan_map, shareable_pairs)
    return neighbour


def tree_plan_vertex_neighbour(state: Tuple[Dict[Pattern, TreePlan], List[List]]):
    """Move a little bit x, from the left or the right."""
    pattern_to_tree_plan_map, shareable_pairs = state
    alg = algoA()
    count_common_pairs = lambda lst: len(lst) if lst is not None else 0
    count_common_pairs_lst = lambda lst_of_lists: sum([count_common_pairs(lst) for lst in lst_of_lists])
    if sum([count_common_pairs_lst(lst_of_lists) for lst_of_lists in shareable_pairs]) == 0:
        return state
    neighbour = alg.Nvertex_neighborhood(pattern_to_tree_plan_map, shareable_pairs)
    return neighbour


iter_num = 1


def tree_plan_state_get_summary(state: Tuple[Dict[Pattern, TreePlan], List[List]]):
    _, shareable_pairs = state
    count_common_pairs = lambda lst: len(lst) if lst is not None else 0
    count_common_pairs_lst = lambda lst_of_lists: sum([count_common_pairs(lst) for lst in lst_of_lists])

    global iter_num
    iter_num += 1
    # dividing by 2 cause the shareable_pairs is a symmetric matrix
    sum_common_pairs = sum([count_common_pairs_lst(lst_of_lists) for lst_of_lists in shareable_pairs]) // 2
    return "iter " + str(iter_num) + " common pairs size = " + str(sum_common_pairs)


def tree_plan_equal(state1: Tuple[Dict[Pattern, TreePlan], List[List]],
                    state2: Tuple[Dict[Pattern, TreePlan], List[List]]):
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
