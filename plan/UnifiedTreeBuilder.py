"""
This file contains the implementations of algorithms constructing a left-deep tree-based evaluation mechanism.
"""
import itertools
import math
from copy import deepcopy
from datetime import timedelta
from typing import List, Dict

from base.Pattern import Pattern
from base.PatternStructure import CompositeStructure, PatternStructure, UnaryStructure, PrimitiveEventStructure
from condition.Condition import Variable
from misc import DefaultConfig
from misc.ConsumptionPolicy import ConsumptionPolicy
from misc.DefaultConfig import DEFAULT_TREE_COST_MODEL
from plan.TreeCostModels import TreeCostModels
from plan.TreePlan import TreePlanLeafNode, TreePlan, TreePlanNode, TreePlanInternalNode, TreePlanUnaryNode, \
    TreePlanBinaryNode
from plan.TreePlanBuilder import TreePlanBuilder
from plan.TreePlanBuilderOrders import TreePlanBuilderOrder
from plan.multi.MultiPatternUnifiedTreePlanApproaches import MultiPatternTreePlanUnionApproaches
from tree.TreeVisualizationUtility import GraphVisualization
from tree.nodes.LeafNode import LeafNode
from tree.nodes.Node import Node


class UnifiedTreeBuilder(TreePlanBuilder):
    """
    A class for deep tree builders.
    """

    def __init__(self, cost_model_type: TreeCostModels = DEFAULT_TREE_COST_MODEL,
                 tree_plan_order_approach: TreePlanBuilderOrder = TreePlanBuilderOrder.LEFT_TREE):
        super(UnifiedTreeBuilder, self).__init__(cost_model_type)
        self.tree_plan_build_approach = tree_plan_order_approach
        self.trees_number_nodes_shared = 0

    @staticmethod
    def get_instance(cost_model_type: TreeCostModels = DEFAULT_TREE_COST_MODEL,
                     tree_plan_order_approach: TreePlanBuilderOrder = TreePlanBuilderOrder.LEFT_TREE):
        return UnifiedTreeBuilder(cost_model_type, tree_plan_order_approach)

    @staticmethod
    def create_ordered_tree_builders():
        """
            Creates a tree plan builders according to the building order
       """
        approaches = TreePlanBuilderOrder.list()
        builders_set = {tree_plan_order: UnifiedTreeBuilder.get_instance(tree_plan_order_approach=tree_plan_order) for
                        tree_plan_order in
                        approaches}
        return builders_set

    def _union_tree_plans(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan,
                          tree_plan_union_approach: MultiPatternTreePlanUnionApproaches):
        if isinstance(pattern_to_tree_plan_map, TreePlan) or len(pattern_to_tree_plan_map) <= 1:
            return pattern_to_tree_plan_map

        pattern_to_tree_plan_map_copy = pattern_to_tree_plan_map.copy()

        if tree_plan_union_approach == MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES:
            return self.__share_leaves(pattern_to_tree_plan_map_copy)
        if tree_plan_union_approach == MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION:
            return self.__construct_subtrees_union_tree_plan(pattern_to_tree_plan_map_copy)
        if tree_plan_union_approach == MultiPatternTreePlanUnionApproaches.TREE_PLAN_CHANGE_TOPOLOGY_UNION:
            return self.__construct_subtrees_change_topology_tree_plan(pattern_to_tree_plan_map_copy)
        else:
            raise Exception("Unsupported union algorithm, yet")

    def visualize(self, visualize_data: TreePlanNode or Dict[Pattern, TreePlan], title=None,
                  visualize_flag=DefaultConfig.VISUALIZATION):
        if visualize_flag and isinstance(visualize_data, TreePlanNode):
            G = GraphVisualization(title)
            G.build_from_root_treePlan(visualize_data, node_level=visualize_data.height)
            G.visualize()

        if visualize_flag and isinstance(visualize_data, dict):
            G = GraphVisualization(title)
            for i, (_, tree_plan) in enumerate(visualize_data.items()):
                G.build_from_root_treePlan(tree_plan.root, node_level=tree_plan.root.height)
            G.visualize()

    def build_ordered_tree_plans(self, patterns: List[Pattern]):
        """
        Creates a tree-based evaluation plan for the given pattern.
        """
        orders = UnifiedTreeBuilder.find_matches_orders(patterns)
        trees = {pattern: TreePlan(self._order_to_tree_topology(orders[i], pattern)) for i, pattern in
                 enumerate(patterns)}
        return trees

    @staticmethod
    def find_matches_orders(patterns: List[Pattern]):
        """
        """
        if len(patterns) <= 1:
            return UnifiedTreeBuilder._create_evaluation_order(patterns[0])

        first_two_orders = UnifiedTreeBuilder.find_orders_for_two_patterns(patterns[0], patterns[1])
        orders = first_two_orders
        for i, pattern in list(enumerate(patterns))[2:]:
            orders += [UnifiedTreeBuilder.find_order_for_new_pattern(patterns[0:i], pattern)]
        return orders

    @staticmethod
    def find_orders_for_two_patterns(pattern1: Pattern, pattern2: Pattern):
        """
        """
        # is_commutative1, is_commutative2 = pattern1.positive_structure.commutative(), pattern2.positive_structure.commutative()
        # if not (is_commutative1 and is_commutative2):
        #     order1 = list(range(len(pattern1.positive_structure.args)))
        #     order2 = list(range(len(pattern2.positive_structure.args)))
        #     return [order1, order2]

        pattern1_events = pattern1.positive_structure.get_args()
        pattern2_events = pattern2.positive_structure.get_args()
        shared = []
        # intersected_names = sorted(set(pattern1_names) & set(pattern2_names))

        for event1 in pattern1_events:
            for event2 in pattern2_events:
                if event1.type == event2.type and UnifiedTreeBuilder.are_conditions_equal(pattern1, event1.name,
                                                                                          pattern2, event2.name):
                    shared += [(event1.name, event2.name, pattern1_events.index(event1), pattern2_events.index(event2))]
                    break
        # for event_name in intersected_names:
        #     if UnifiedTreeBuilder.are_con_equal(pattern1, pattern2, event_name):
        #         shared += [(event_name, pattern1_names.index(event_name), pattern2_names.index(event_name))]

        if len(shared) == 0:
            order1 = list(range(len(pattern1.positive_structure.args)))
            order2 = list(range(len(pattern2.positive_structure.args)))
            return [order1, order2]

        names1, names2, order1, order2 = list(zip(*shared))
        order1 = list(order1) + list(filter(lambda x: x not in order1, range(len(pattern1_events))))
        order2 = list(order2) + list(filter(lambda x: x not in order2, range(len(pattern2_events))))
        return [order1, order2]

    @staticmethod
    def are_conditions_equal(pattern1, event_name1, pattern2, event_name2):
        condition1 = pattern1.condition.get_condition_of(event_name1, get_kleene_closure_conditions=False,
                                                         consume_returned_conditions=False)
        condition2 = pattern2.condition.get_condition_of(event_name2, get_kleene_closure_conditions=False,
                                                         consume_returned_conditions=False)
        return condition1 == condition2

    @staticmethod
    def find_order_for_new_pattern(patterns: List[Pattern], new_pattern: Pattern):
        """
        """

        max_order_len = - math.inf
        best_order = []
        for i, pattern in enumerate(patterns):
            _, order2 = UnifiedTreeBuilder.find_orders_for_two_patterns(pattern, new_pattern)
            if len(order2) >= max_order_len:
                max_order_len = len(order2)
                best_order = order2

        return best_order

    def _create_tree_topology(self, pattern: Pattern):
        """
        Invokes an algorithm (to be implemented by subclasses) that builds an evaluation order of the operands, and
        converts it into a left-deep tree topology.
        """
        order = UnifiedTreeBuilder._create_evaluation_order(pattern) if isinstance(pattern.positive_structure,
                                                                                   CompositeStructure) else [0]
        return self._order_to_tree_topology(order, pattern)

    def _order_to_tree_topology(self, order: List[int], pattern: Pattern):

        if self.tree_plan_build_approach == TreePlanBuilderOrder.LEFT_TREE:
            return UnifiedTreeBuilder._order_to_tree_topology_left(order, pattern)

        if self.tree_plan_build_approach == TreePlanBuilderOrder.RIGHT_TREE:
            return UnifiedTreeBuilder._order_to_tree_topology_right(order, pattern)

        if self.tree_plan_build_approach == TreePlanBuilderOrder.BALANCED_TREE:
            return UnifiedTreeBuilder._order_to_tree_topology_balanced(order, pattern)

        else:
            raise Exception("Unsupported tree topology build algorithm, yet")

    @staticmethod
    def _create_evaluation_order(pattern: Pattern):
        args_num = len(pattern.positive_structure.args)
        is_commutative = pattern.positive_structure.commutative()
        if is_commutative:
            return list(
                map(lambda t: t[0], sorted(enumerate(pattern.positive_structure.args), key=lambda t: t[1].name)))

        return list(range(args_num))

    @staticmethod
    def _order_to_tree_topology_balanced(order: List[int], pattern: Pattern):
        """
        A helper method for converting a given order to a balanced tree topology.
        """
        if len(order) <= 1:
            return TreePlanLeafNode(order[0])
        tree_topology1 = UnifiedTreeBuilder._order_to_tree_topology_balanced(order[0:len(order) // 2], pattern)
        tree_topology2 = UnifiedTreeBuilder._order_to_tree_topology_balanced(order[len(order) // 2:], pattern)
        tree_topology = UnifiedTreeBuilder._instantiate_binary_node(pattern, tree_topology1, tree_topology2)
        return tree_topology

    @staticmethod
    def _order_to_tree_topology_left(order: List[int], pattern: Pattern):
        """
        A helper method for converting a given order to a left-deep tree topology.
        """
        if len(order) <= 1:
            return TreePlanLeafNode(order[0])
        tree_topology = TreePlanLeafNode(order[0])
        for i in range(1, len(order)):
            tree_topology = UnifiedTreeBuilder._instantiate_binary_node(pattern, tree_topology,
                                                                        TreePlanLeafNode(order[i]))
        return tree_topology

    @staticmethod
    def _order_to_tree_topology_right(order: List[int], pattern: Pattern):
        """
        A helper method for converting a given order to a right-deep tree topology.
        """
        if len(order) <= 1:
            return TreePlanLeafNode(order[0])
        tree_topology = TreePlanLeafNode(order[len(order) - 1])
        for i in range(len(order) - 2, -1, -1):
            tree_topology = UnifiedTreeBuilder._instantiate_binary_node(pattern, TreePlanLeafNode(order[i]),
                                                                        tree_topology)
        return tree_topology

    @staticmethod
    def _order_to_tree_topology_half_left_half_balanced(order: List[int], pattern: Pattern):
        """
        A helper method for converting a given order to a right-deep tree topology.
        """
        if len(order) <= 1:
            return TreePlanLeafNode(order[0])

        tree_topology1 = UnifiedTreeBuilder._order_to_tree_topology_left(order[0:len(order) // 2], pattern)
        tree_topology2 = UnifiedTreeBuilder._order_to_tree_topology_balanced(order[len(order) // 2:], pattern)
        tree_topology = UnifiedTreeBuilder._instantiate_binary_node(pattern, tree_topology1, tree_topology2)
        return tree_topology

    def __try_to_share_and_merge_nodes(self, root, root_pattern: Pattern, node, node_pattern: Pattern):
        """
            This method is trying to share the node (and its subtree) and the tree of root.
            If the root and node are not equivalent, trying to share the children of node and root.
        """
        if root is None or node is None:
            return root, 0

        merged_node, number_merged = self.__find_and_merge_node_into_subtree(root, root_pattern, node, node_pattern)

        if number_merged:
            return merged_node, number_merged

        if isinstance(node, TreePlanBinaryNode):
            return self.__try_to_share_and_merge_nodes_binary(root, root_pattern, node, node_pattern)

        if isinstance(node, TreePlanUnaryNode):
            updated_root, number_merged = self.__try_to_share_and_merge_nodes(root, root_pattern, node.child,
                                                                              node_pattern)
            if number_merged:
                return updated_root, number_merged

        return root, 0

    def __try_to_share_and_merge_nodes_binary(self, root, root_pattern: Pattern, node, node_pattern: Pattern):
        # try left merge
        left_side_new_root, number_merged_left = self.__try_to_share_and_merge_nodes(root, root_pattern,
                                                                                     node.left_child, node_pattern)
        if number_merged_left > 0:
            right_side_new_root, number_merged_right = self.__try_to_share_and_merge_nodes(root, root_pattern,
                                                                                           node.right_child,
                                                                                           node_pattern)
            if number_merged_right > 0:
                return right_side_new_root, number_merged_left + number_merged_right
            return left_side_new_root, number_merged_left + number_merged_right

        right_side_new_root, number_merged_right = self.__try_to_share_and_merge_nodes(root, root_pattern,
                                                                                       node.right_child, node_pattern)
        if number_merged_right > 0:
            return right_side_new_root, number_merged_right
        return root, 0

    @staticmethod
    def _sub_tree_size(root):

        if root is None:
            return 0

        if isinstance(root, TreePlanLeafNode):
            return 1

        if isinstance(root, TreePlanBinaryNode):
            return 1 + UnifiedTreeBuilder._sub_tree_size(root.left_child) + UnifiedTreeBuilder._sub_tree_size(
                root.right_child)

        if isinstance(root, TreePlanUnaryNode):
            return 1 + UnifiedTreeBuilder._sub_tree_size(root.child)

        raise Exception("Unsupported tree plan node type")

    def __find_and_merge_node_into_subtree(self, root: TreePlanNode, root_pattern: Pattern, node: TreePlanNode,
                                           node_pattern: Pattern):
        """
               This method is trying to find node in the subtree of root (or an equivalent node).
               If such a node is found, it merges the equivalent nodes.
        """
        if UnifiedTreeBuilder.is_equivalent(root, root_pattern, node, node_pattern, self.leaves_dict):
            if type(root) == TreePlanLeafNode:
                events = self.leaves_dict[node_pattern][node]
                self.leaves_dict[node_pattern].pop(node)
                self.leaves_dict[node_pattern][root] = events
            return node, UnifiedTreeBuilder._sub_tree_size(node)

        elif isinstance(root, TreePlanBinaryNode):
            left_find_and_merge, number_of_merged_left = self.__find_and_merge_node_into_subtree(root.left_child,
                                                                                                 root_pattern, node,
                                                                                                 node_pattern)
            right_find_and_merge, number_of_merged_right = self.__find_and_merge_node_into_subtree(root.right_child,
                                                                                                   root_pattern, node,
                                                                                                   node_pattern)

            if number_of_merged_left:
                root.left_child = left_find_and_merge

            if number_of_merged_right:
                root.right_child = right_find_and_merge

            return root, number_of_merged_left + number_of_merged_right

        elif isinstance(root, TreePlanUnaryNode):
            child_find_and_merge, is_child_merged = self.__find_and_merge_node_into_subtree(root.child, root_pattern,
                                                                                            node, node_pattern)

            if is_child_merged:
                root.child = child_find_and_merge

            return root, is_child_merged

        return root, 0

    def __construct_subtrees_change_topology_tree_plan(self,
                                                       pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan):
        """
        This method gets patterns, builds a single-pattern tree to each one of them,
        and merges equivalent subtrees from different trees.
        We are sharing the maximal subtree that exists in the tree.
        Additional: we change tree plan topology trying to get maximum union
        We are assuming that each pattern appears only once in patterns (which is a legitimate assumption).
        """
        if len(pattern_to_tree_plan_map) <= 1:
            return pattern_to_tree_plan_map
        patterns = list(pattern_to_tree_plan_map.keys())
        pattern1, pattern2 = patterns[0], patterns[1]
        unified_tree_map, max_intersection = self._two_patterns_max_merge(pattern1, pattern2, pattern_to_tree_plan_map)

        self.trees_number_nodes_shared = max_intersection
        best_orders = self.find_matches_orders(list(pattern_to_tree_plan_map.keys()))
        if len(pattern_to_tree_plan_map) <= 2:
            return unified_tree_map

        for i, pattern in list(enumerate(patterns))[2:]:
            current_unified_tree_map, max_intersection = self._append_pattern_to_multi_tree(i, unified_tree_map,
                                                                                            best_orders[i],
                                                                                            pattern_to_tree_plan_map)
            unified_tree_map[pattern] = current_unified_tree_map[pattern]
            self.trees_number_nodes_shared += max_intersection
        return unified_tree_map

    def _two_patterns_max_merge(self, pattern1: Pattern, pattern2: Pattern,
                                pattern_to_tree_plan_map: Dict[Pattern, TreePlan]):
        """
                This method gets two patterns, and tree to each one of them,
                and merges equivalent subtrees from different trees. then we try changing topology and merge again
                We are sharing the maximal subtree that exists in the tree.
        """
        builders = UnifiedTreeBuilder.create_ordered_tree_builders()
        tree_plan_build_approaches = builders.keys()
        order1, order2 = self.find_matches_orders([pattern1, pattern2])

        union_builder = UnifiedTreeBuilder()
        max_intersection = - math.inf

        best_unified = None
        for approach1, approach2 in itertools.product(tree_plan_build_approaches, tree_plan_build_approaches):
            builder1 = builders.get(approach1)
            builder2 = builders.get(approach2)
            tree1 = TreePlan(builder1._order_to_tree_topology(order1, pattern1))
            tree2 = TreePlan(builder2._order_to_tree_topology(order2, pattern2))
            tree1_size = builder1._sub_tree_size(tree1.root)
            tree2_size = builder2._sub_tree_size(tree2.root)
            pattern_to_tree_plan_map = {pattern1: tree1, pattern2: tree2}
            unified = union_builder._union_tree_plans(pattern_to_tree_plan_map.copy(),
                                                      MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION)
            trees_number_nodes_shared = union_builder.trees_number_nodes_shared
            if trees_number_nodes_shared > max_intersection:
                max_intersection, best_approach1, best_approach2 = trees_number_nodes_shared, approach1, approach2
                best_unified = unified
            if max_intersection >= min(tree1_size, tree2_size):
                # we got the max intersection that could be
                break

        return best_unified, max_intersection

    def _append_pattern_to_multi_tree(self, pattern_idx, unified_pattern_to_tree_plan_map, best_order: List[int],
                                      pattern_to_tree_plan_map):
        """
                This method gets two pattern_to_tree_plan_map, and pattern,
                and merges equivalent subtrees from different pattern tree plan to the const unified multi tree of the other patterns.
                then we try changing topology and merge again
                We are sharing the maximal subtree that exists in the tree.
        """
        builders = UnifiedTreeBuilder.create_ordered_tree_builders()
        tree_plan_build_approaches = builders.keys()

        patterns = list(pattern_to_tree_plan_map.keys())
        current_pattern = patterns[pattern_idx]

        best_unified_tree_map, max_intersection = None, -math.inf
        for pattern in unified_pattern_to_tree_plan_map:
            unified_tree_map, cur_intersection = self._two_patterns_max_merge(pattern, current_pattern,
                                                                              pattern_to_tree_plan_map)
            if cur_intersection >= max_intersection:
                max_intersection = cur_intersection
                best_unified_tree_map = unified_tree_map.copy()

        for pattern in unified_pattern_to_tree_plan_map:
            for approach in tree_plan_build_approaches:
                builder = builders.get(approach)
                tree = TreePlan(builder._order_to_tree_topology(best_order, current_pattern))
                tree_size = builder._sub_tree_size(tree.root)

                pattern_to_tree_plan_map[current_pattern] = tree
                unified_tree_map, cur_intersection = self._two_patterns_max_merge(pattern, current_pattern,
                                                                                  pattern_to_tree_plan_map)

                if cur_intersection > max_intersection:
                    max_intersection = cur_intersection
                    best_unified_tree_map = unified_tree_map.copy()

                if max_intersection >= tree_size:
                    # we got the max intersection that could be
                    break

        return best_unified_tree_map, max_intersection

    def __construct_subtrees_union_tree_plan(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan):
        """
        This method gets patterns, builds a single-pattern tree to each one of them,
        and merges equivalent subtrees from different trees.
        We are sharing the maximal subtree that exists in the tree.
        We are assuming that each pattern appears only once in patterns (which is a legitimate assumption).
        """
        self.trees_number_nodes_shared = 0
        leaves_dict = {}
        for i, pattern in enumerate(pattern_to_tree_plan_map):
            tree_plan_leaves_pattern = pattern_to_tree_plan_map[pattern].root.get_leaves()
            pattern_event_size = len(pattern.positive_structure.get_args())
            leaves_dict[pattern] = {tree_plan_leaves_pattern[i]: pattern.positive_structure.get_args()[i] for i in
                                    range(pattern_event_size)}

        self.leaves_dict = leaves_dict

        output_nodes = {}
        for i, pattern in enumerate(pattern_to_tree_plan_map):
            tree_plan = pattern_to_tree_plan_map[pattern]
            current_root = tree_plan.root
            output_nodes[current_root] = pattern
            for root in output_nodes:
                if root == current_root:
                    break
                new_root, number_shared = self.__try_to_share_and_merge_nodes(current_root, pattern, root,
                                                                              output_nodes[root])
                if number_shared:
                    self.trees_number_nodes_shared += number_shared
                    pattern_to_tree_plan_map[pattern].root = new_root
                    break

        return pattern_to_tree_plan_map

    def __share_leaves(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan):
        """ this function is the core implementation for the trivial_sharing_trees algorithm """
        self.trees_number_nodes_shared = 0
        leaves_dict = {}
        first_pattern = list(pattern_to_tree_plan_map.keys())[0]
        for i, pattern in enumerate(pattern_to_tree_plan_map):
            tree_plan_leaves_pattern = pattern_to_tree_plan_map[pattern].root.get_leaves()
            leaves_dict[pattern] = {tree_plan_leaves_pattern[i]: pattern.positive_structure.get_args()[i] for i in
                                    range(0, len(tree_plan_leaves_pattern))}

        shared_leaves_dict = {}
        for leaf in leaves_dict[first_pattern]:
            shared_leaves_dict[leaf] = [first_pattern,
                                        TreePlanLeafNode(leaf.event_index, leaf.event_type, leaf.event_name)]
        for pattern in list(pattern_to_tree_plan_map)[1:]:
            curr_leaves = leaves_dict[pattern]
            for curr_leaf in curr_leaves.keys():
                leaf_tree_plan_node = TreePlanLeafNode(curr_leaf.event_index, curr_leaf.event_type,
                                                       curr_leaf.event_name)
                for leaf in shared_leaves_dict.keys():
                    leaf_pattern, _ = shared_leaves_dict[leaf]
                    curr_leaf_pattern = pattern
                    event1 = leaves_dict[curr_leaf_pattern][curr_leaf]
                    event2 = leaves_dict[leaf_pattern][leaf]

                    condition1, condition2 = UnifiedTreeBuilder.get_condition_from_pattern_in_sub_tree(curr_leaf,
                                                                                                       curr_leaf_pattern,
                                                                                                       leaf,
                                                                                                       leaf_pattern,
                                                                                                       leaves_dict)
                    if condition1 is None or condition2 is None:
                        continue
                    if condition1 == condition2 and event1.type == event2.type:
                        self.trees_number_nodes_shared += 1
                        _, leaf_tree_plan_node = shared_leaves_dict[leaf]
                        break
                shared_leaves_dict[curr_leaf] = [pattern, leaf_tree_plan_node]
        return UnifiedTreeBuilder._tree_plans_update_leaves(pattern_to_tree_plan_map,
                                                            shared_leaves_dict)  # the unified tree

    # TODO : MST
    @staticmethod
    def __get_operator_arg_list(operator: PatternStructure):
        """
        Returns the list of arguments of the given operator for the tree construction process.
        """
        if isinstance(operator, CompositeStructure):
            return operator.args
        if isinstance(operator, UnaryStructure):
            return [operator.arg]
        # a PrimitiveEventStructure
        return [operator]

    @staticmethod
    def get_condition_from_pattern_in_one_sub_tree(plan_node: TreePlanNode, pattern: Pattern, leaves_dict):

        leaves_in_plan_node_1 = plan_node.get_leaves()
        if leaves_in_plan_node_1 is None:
            return None
        event_indexes1 = list(map(lambda e: e.event_index, leaves_in_plan_node_1))
        pattern1_events = list(leaves_dict.get(pattern).values())
        names1 = {pattern1_events[event_index].name for event_index in event_indexes1}
        return deepcopy(pattern.condition.get_condition_of(names1, get_kleene_closure_conditions=False,
                                                                  consume_returned_conditions=False))

    @staticmethod
    def get_condition_from_pattern_in_sub_tree(plan_node1: TreePlanNode, pattern1: Pattern, plan_node2: TreePlanNode,
                                               pattern2: Pattern,
                                               leaves_dict):

        leaves_in_plan_node_1 = plan_node1.get_leaves()
        leaves_in_plan_node_2 = plan_node2.get_leaves()
        if leaves_in_plan_node_1 is None or leaves_in_plan_node_2 is None:
            return None, None

        event_indexes1 = list(map(lambda e: e.event_index, leaves_in_plan_node_1))
        pattern1_events = list(leaves_dict.get(pattern1).values())

        event_indexes2 = list(map(lambda e: e.event_index, leaves_in_plan_node_2))
        pattern2_events = list(leaves_dict.get(pattern2).values())

        names1 = {pattern1_events[event_index].name for event_index in event_indexes1}
        names2 = {pattern2_events[event_index].name for event_index in event_indexes2}

        condition1 = deepcopy(pattern1.condition.get_condition_of(names1, get_kleene_closure_conditions=False,
                                                                  consume_returned_conditions=False))
        condition2 = deepcopy(pattern2.condition.get_condition_of(names2, get_kleene_closure_conditions=False,
                                                                  consume_returned_conditions=False))

        if condition1 == condition2:
            return condition1, condition2

        event1_name_type = {event.name: event.type for event in pattern1_events}
        event2_name_type = {event.name: event.type for event in pattern2_events}


        for e in condition1._CompositeCondition__conditions:
            if type(e.left_term_repr) == Variable:
                e.left_term_repr.name = event1_name_type[e.left_term_repr.name]
            if type(e.right_term_repr) == Variable:
                e.right_term_repr.name = event1_name_type[e.right_term_repr.name]
        for e in condition2._CompositeCondition__conditions:
            if type(e.left_term_repr) == Variable:
                e.left_term_repr.name = event2_name_type[e.left_term_repr.name]
            if type(e.right_term_repr) == Variable:
                e.right_term_repr.name = event2_name_type[e.right_term_repr.name]

        return condition1, condition2

    @staticmethod
    def is_equivalent(plan_node1: TreePlanNode, pattern1: Pattern, plan_node2: TreePlanNode, pattern2: Pattern,
                      leaves_dict: Dict[Pattern, Dict[TreePlanNode, PrimitiveEventStructure]]):

        if type(plan_node1) != type(plan_node2) or plan_node1 is None or plan_node2 is None:
            return False

        condition1, condition2 = UnifiedTreeBuilder.get_condition_from_pattern_in_sub_tree(plan_node1, pattern1,
                                                                                           plan_node2, pattern2,
                                                                                           leaves_dict)

        if condition1 is None or condition2 is None or condition1 != condition2:
            return False

        nodes_type = type(plan_node1)

        if issubclass(nodes_type, TreePlanInternalNode):
            if plan_node1.operator != plan_node2.operator:
                return False
            if nodes_type == TreePlanUnaryNode:
                return UnifiedTreeBuilder.is_equivalent(plan_node1.child, pattern1, plan_node2.child, pattern2,
                                                        leaves_dict)

            if nodes_type == TreePlanBinaryNode:
                return UnifiedTreeBuilder.is_equivalent(plan_node1.left_child, pattern1, plan_node2.left_child,
                                                        pattern2, leaves_dict) \
                       and UnifiedTreeBuilder.is_equivalent(plan_node1.right_child, pattern1, plan_node2.right_child,
                                                            pattern2, leaves_dict)

        if nodes_type == TreePlanLeafNode:
            event1 = leaves_dict.get(pattern1).get(plan_node1)
            event2 = leaves_dict.get(pattern2).get(plan_node2)
            if event1 and event2:
                return event1.type == event2.type

        return False

    @staticmethod
    def tree_get_leaves(root_operator: PatternStructure, tree_plan: TreePlanNode,
                        args: List[PatternStructure], sliding_window: timedelta,
                        consumption_policy: ConsumptionPolicy):
        if tree_plan is None:
            return {}
        leaves_nodes = {}
        if type(tree_plan) == TreePlanLeafNode:
            # a special case where the top operator of the entire pattern is an unary operator
            leaves_nodes[tree_plan] = LeafNode(sliding_window, tree_plan.event_index,
                                               root_operator.args[tree_plan.event_index], None)
            return leaves_nodes
        leaves_nodes = UnifiedTreeBuilder.tree_get_leaves(root_operator, tree_plan.left_child, args, sliding_window,
                                                          consumption_policy)
        leaves_nodes.update(
            UnifiedTreeBuilder.tree_get_leaves(root_operator, tree_plan.right_child, args, sliding_window,
                                               consumption_policy))
        return leaves_nodes

    @staticmethod
    def _tree_plans_update_leaves(pattern_to_tree_plan_map: Dict[Pattern, TreePlan], shared_leaves_dict):
        """at this function we share same leaves in different patterns to share the same root ,
        that says one instance of each unique leaf is created ,
        (if a leaf exists in two trees , the leaf will have two parents)
        """
        for pattern, tree_plan in pattern_to_tree_plan_map.items():
            updated_tree_plan_root = UnifiedTreeBuilder._single_tree_plan_update_leaves(tree_plan.root,
                                                                                        shared_leaves_dict)
            pattern_to_tree_plan_map[pattern].root = updated_tree_plan_root
        return pattern_to_tree_plan_map

    @staticmethod
    def _single_tree_plan_update_leaves(tree_plan_root_node: TreePlanNode, shared_leaves_dict):

        if tree_plan_root_node is None:
            return tree_plan_root_node

        if type(tree_plan_root_node) == TreePlanLeafNode:
            pattern, updated_tree_plan_leaf = shared_leaves_dict[tree_plan_root_node]
            tree_plan_root_node = updated_tree_plan_leaf
            return tree_plan_root_node

        assert issubclass(type(tree_plan_root_node), TreePlanInternalNode)

        if type(tree_plan_root_node) == TreePlanUnaryNode:
            updated_child = UnifiedTreeBuilder._single_tree_plan_update_leaves(tree_plan_root_node.child,
                                                                               shared_leaves_dict)
            tree_plan_root_node.child = updated_child
            return tree_plan_root_node

        if type(tree_plan_root_node) == TreePlanBinaryNode:
            updated_left_child = UnifiedTreeBuilder._single_tree_plan_update_leaves(tree_plan_root_node.left_child,
                                                                                    shared_leaves_dict)
            updated_right_child = UnifiedTreeBuilder._single_tree_plan_update_leaves(tree_plan_root_node.right_child,
                                                                                     shared_leaves_dict)
            tree_plan_root_node.left_child = updated_left_child
            tree_plan_root_node.right_child = updated_right_child
            return tree_plan_root_node

        else:
            raise Exception("Unsupported Node type")
