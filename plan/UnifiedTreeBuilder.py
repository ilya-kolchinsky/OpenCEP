"""
This file contains the implementations of algorithms constructing a left-deep tree-based evaluation mechanism.
"""
import itertools
from datetime import timedelta
from typing import List, Dict

from base.Pattern import Pattern
from base.PatternStructure import CompositeStructure, PatternStructure, UnaryStructure
from misc import DefaultConfig
from misc.ConsumptionPolicy import ConsumptionPolicy
from misc.DefaultConfig import DEFAULT_TREE_COST_MODEL
from plan.TreeCostModels import TreeCostModels
from plan.TreePlan import TreePlanLeafNode, TreePlan, TreePlanNode, TreePlanInternalNode, TreePlanUnaryNode, TreePlanBinaryNode
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
        builders_set = {tree_plan_order: UnifiedTreeBuilder.get_instance(tree_plan_order_approach=tree_plan_order) for tree_plan_order in
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

    def visualize(self, visualize_data: TreePlanNode or Dict[Pattern, TreePlan], title=None, visualize_flag=DefaultConfig.VISUALIZATION):
        if visualize_flag and isinstance(visualize_data, TreePlanNode):
            G = GraphVisualization(title)
            G.build_from_root_treePlan(visualize_data, node_level=visualize_data.height)
            G.visualize()

        if visualize_flag and isinstance(visualize_data, dict):
            G = GraphVisualization(title)
            for i, (_, tree_plan) in enumerate(visualize_data.items()):
                G.build_from_root_treePlan(tree_plan.root, node_level=tree_plan.root.height)
            G.visualize()

    def _create_tree_topology(self, pattern: Pattern):
        """
        Invokes an algorithm (to be implemented by subclasses) that builds an evaluation order of the operands, and
        converts it into a left-deep tree topology.
        """
        order = UnifiedTreeBuilder._create_evaluation_order(pattern) if isinstance(pattern.positive_structure, CompositeStructure) else [0]
        return self._order_to_tree_topology(order, pattern)

    def _order_to_tree_topology(self, order: List[int], pattern: Pattern):

        if self.tree_plan_build_approach == TreePlanBuilderOrder.LEFT_TREE:
            return UnifiedTreeBuilder._order_to_tree_topology_left(order, pattern)

        if self.tree_plan_build_approach == TreePlanBuilderOrder.RIGHT_TREE:
            return UnifiedTreeBuilder._order_to_tree_topology_right(order, pattern)

        if self.tree_plan_build_approach == TreePlanBuilderOrder.BALANCED_TREE:
            return UnifiedTreeBuilder._order_to_tree_topology_balanced(order, pattern)

        if self.tree_plan_build_approach == TreePlanBuilderOrder.HALF_LEFT_HALF_BALANCED_TREE:
            return UnifiedTreeBuilder._order_to_tree_topology_half_left_half_balanced(order, pattern)

        else:
            raise Exception("Unsupported tree topology build algorithm, yet")

    @staticmethod
    def _create_evaluation_order(pattern: Pattern):
        args_num = len(pattern.positive_structure.args)
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
            tree_topology = UnifiedTreeBuilder._instantiate_binary_node(pattern, tree_topology, TreePlanLeafNode(order[i]))
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
            tree_topology = UnifiedTreeBuilder._instantiate_binary_node(pattern, TreePlanLeafNode(order[i]), tree_topology)
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
            updated_root, number_merged = self.__try_to_share_and_merge_nodes(root, root_pattern, node.child, node_pattern)
            if number_merged:
                return updated_root, number_merged

        return root, 0

    def __try_to_share_and_merge_nodes_binary(self, root, root_pattern: Pattern, node, node_pattern: Pattern):
        # try left merge
        left_side_new_root, number_merged_left = self.__try_to_share_and_merge_nodes(root, root_pattern, node.left_child, node_pattern)
        if number_merged_left > 0:
            right_side_new_root, number_merged_right = self.__try_to_share_and_merge_nodes(root, root_pattern, node.right_child, node_pattern)
            if number_merged_right > 0:
                return right_side_new_root, number_merged_left + number_merged_right
            return left_side_new_root, number_merged_left + number_merged_right

        right_side_new_root, number_merged_right = self.__try_to_share_and_merge_nodes(root, root_pattern, node.right_child, node_pattern)
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
            return 1 + UnifiedTreeBuilder._sub_tree_size(root.left_child) + UnifiedTreeBuilder._sub_tree_size(root.right_child)

        if isinstance(root, TreePlanUnaryNode):
            return 1 + UnifiedTreeBuilder._sub_tree_size(root.child)

        raise Exception("Unsupported tree plan node type")

    def __find_and_merge_node_into_subtree(self, root: TreePlanNode, root_pattern: Pattern, node: TreePlanNode, node_pattern: Pattern):
        """
               This method is trying to find node in the subtree of root (or an equivalent node).
               If such a node is found, it merges the equivalent nodes.
        """
        if UnifiedTreeBuilder.is_equivalent(root, root_pattern, node, node_pattern, self.leaves_dict):
            return root, UnifiedTreeBuilder._sub_tree_size(root)

        elif isinstance(root, TreePlanBinaryNode):
            left_find_and_merge, number_of_merged_left = self.__find_and_merge_node_into_subtree(root.left_child, root_pattern, node, node_pattern)
            right_find_and_merge, number_of_merged_right = self.__find_and_merge_node_into_subtree(root.right_child, root_pattern, node, node_pattern)

            if number_of_merged_left:
                root.left_child = left_find_and_merge

            if number_of_merged_right:
                root.right_child = right_find_and_merge

            return root, number_of_merged_left + number_of_merged_right

        elif isinstance(root, TreePlanUnaryNode):
            child_find_and_merge, is_child_merged = self.__find_and_merge_node_into_subtree(root.child, root_pattern, node, node_pattern)

            if is_child_merged:
                root.child = child_find_and_merge

            return root, is_child_merged

        return root, 0

    def __construct_subtrees_change_topology_tree_plan(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan):
        """
        This method gets patterns, builds a single-pattern tree to each one of them,
        and merges equivalent subtrees from different trees.
        We are sharing the maximal subtree that exists in the tree.
        Additional: we change tree plan topology trying to get maximum union
        We are assuming that each pattern appears only once in patterns (which is a legitimate assumption).
        """
        builders = UnifiedTreeBuilder.create_ordered_tree_builders()

        tree_plan_build_approaches = builders.keys()

        multi_pattern_eval_approach = MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION

        unified_tree_map, max_intersection = self._first_two_patterns_max_merge(pattern_to_tree_plan_map)
        self.trees_number_nodes_shared = max_intersection

        if len(pattern_to_tree_plan_map) <= 2:
            return unified_tree_map

        for i, pattern in list(enumerate(pattern_to_tree_plan_map))[2:]:
            unified_tree_map, max_intersection = self._append_pattern_to_multi_tree(i, unified_tree_map, pattern_to_tree_plan_map)
            self.trees_number_nodes_shared += max_intersection
        return unified_tree_map

    def _first_two_patterns_max_merge(self, pattern_to_tree_plan_map:  Dict[Pattern, TreePlan]):
        """
                This method gets two patterns, and tree to each one of them,
                and merges equivalent subtrees from different trees. then we try changing topology and merge again
                We are sharing the maximal subtree that exists in the tree.
        """
        builders = UnifiedTreeBuilder.create_ordered_tree_builders()
        tree_plan_build_approaches = builders.keys()

        patterns = list(pattern_to_tree_plan_map.keys())
        if len(patterns) <= 1:
            return pattern_to_tree_plan_map
        pattern1, pattern2 = patterns[0], patterns[1]

        union_builder = UnifiedTreeBuilder()
        sub_pattern_to_tree_plan_map = {pattern1: pattern_to_tree_plan_map[pattern1], pattern2: pattern_to_tree_plan_map[pattern2]}
        _ = union_builder._union_tree_plans(sub_pattern_to_tree_plan_map, MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION)
        max_intersection = union_builder.trees_number_nodes_shared

        unified = None
        best_approach1, best_approach2 = None, None
        for approach1, approach2 in itertools.product(tree_plan_build_approaches, tree_plan_build_approaches):
            builder1 = builders.get(approach1)
            builder2 = builders.get(approach2)
            tree1 = builder1.build_tree_plan(pattern1)
            tree2 = builder2.build_tree_plan(pattern2)
            tree1_size = builder1._sub_tree_size(tree1.root)
            tree2_size = builder2._sub_tree_size(tree2.root)
            pattern_to_tree_plan_map = {pattern1: tree1, pattern2: tree2}
            unified = union_builder._union_tree_plans(pattern_to_tree_plan_map.copy(), MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION)
            trees_number_nodes_shared = union_builder.trees_number_nodes_shared
            if trees_number_nodes_shared > max_intersection:
                max_intersection, best_approach1, best_approach2 = trees_number_nodes_shared, approach1, approach2
            if max_intersection >= min(tree1_size, tree2_size):
                # we got the max intersection that could be
                break

        return unified, max_intersection

    def _append_pattern_to_multi_tree(self, pattern_idx, unified_pattern_to_tree_plan_map, pattern_to_tree_plan_map):
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
        current_tree = pattern_to_tree_plan_map[current_pattern]

        unified_pattern_to_tree_plan_map[current_pattern] = current_tree

        union_builder = UnifiedTreeBuilder()
        unified_tree_map = union_builder._union_tree_plans(unified_pattern_to_tree_plan_map,
                                                           MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION)
        max_intersection = union_builder.trees_number_nodes_shared

        for approach in tree_plan_build_approaches:
            builder = builders.get(approach)
            tree = builder.build_tree_plan(current_pattern)
            tree_size = builder._sub_tree_size(tree.root)
            _ = union_builder._union_tree_plans(unified_tree_map, MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION)
            trees_number_nodes_shared = union_builder.trees_number_nodes_shared
            if trees_number_nodes_shared > max_intersection:
                max_intersection = trees_number_nodes_shared
            if max_intersection >= tree_size:
                # we got the max intersection that could be
                break

        return unified_tree_map, max_intersection

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
            tree_plan = pattern_to_tree_plan_map[pattern]
            leaves_dict[pattern] = UnifiedTreeBuilder.tree_get_leaves(pattern.positive_structure, tree_plan.root,
                                                                      UnifiedTreeBuilder.__get_operator_arg_list(pattern.positive_structure),
                                                                      pattern.window, pattern.consumption_policy)

        self.leaves_dict = leaves_dict

        output_nodes = {}
        for i, pattern in enumerate(pattern_to_tree_plan_map):
            tree_plan = pattern_to_tree_plan_map[pattern]
            current_root = tree_plan.root
            output_nodes[current_root] = pattern
            for root in output_nodes:
                if root == current_root:
                    break
                new_root, number_shared = self.__try_to_share_and_merge_nodes(current_root, pattern, root, output_nodes[root])
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
            tree_plan = pattern_to_tree_plan_map[pattern]
            get_args = UnifiedTreeBuilder.__get_operator_arg_list
            leaves_dict[pattern] = UnifiedTreeBuilder.tree_get_leaves(pattern.positive_structure, tree_plan.root,
                                                                      get_args(pattern.positive_structure),
                                                                      pattern.window, pattern.consumption_policy)
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
                    curr_tree_node = leaves_dict[curr_leaf_pattern][curr_leaf]
                    leaf_tree_node = leaves_dict[leaf_pattern][leaf]

                    condition1, condition2 = UnifiedTreeBuilder.get_condition_from_pattern_in_sub_tree(curr_leaf, curr_leaf_pattern, leaf,
                                                                                                       leaf_pattern,
                                                                                                       leaves_dict)

                    if condition1 == condition2 and curr_tree_node.get_event_name() == leaf_tree_node.get_event_name() \
                            and curr_tree_node.is_equivalent(leaf_tree_node):
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
    def get_condition_from_pattern_in_sub_tree(plan_node1: TreePlanNode, pattern1: Pattern, plan_node2: TreePlanNode, pattern2: Pattern,
                                               leaves_dict: Dict[Pattern, Dict[TreePlanNode, Node]]):

        leaves_in_plan_node_1 = plan_node1.get_leaves()
        leaves_in_plan_node_2 = plan_node2.get_leaves()

        names1 = {leaves_dict.get(pattern1).get(plan_leaf).get_structure_summary() for plan_leaf in
                  leaves_dict.get(pattern1).fromkeys(leaves_in_plan_node_1)}
        names2 = {leaves_dict.get(pattern2).get(plan_leaf).get_structure_summary() for plan_leaf in
                  leaves_dict.get(pattern2).fromkeys(leaves_in_plan_node_2)}

        condition1 = pattern1.condition.get_condition_of(names1, get_kleene_closure_conditions=False, consume_returned_conditions=False)
        condition2 = pattern2.condition.get_condition_of(names2, get_kleene_closure_conditions=False, consume_returned_conditions=False)
        return condition1, condition2

    @staticmethod
    def is_equivalent(plan_node1: TreePlanNode, pattern1: Pattern, plan_node2: TreePlanNode, pattern2: Pattern,
                      leaves_dict: Dict[Pattern, Dict[TreePlanNode, Node]]):

        if type(plan_node1) != type(plan_node2) or plan_node1 is None or plan_node2 is None:
            return False

        condition1, condition2 = UnifiedTreeBuilder.get_condition_from_pattern_in_sub_tree(plan_node1, pattern1, plan_node2, pattern2, leaves_dict)

        if condition1 != condition2:
            return False

        nodes_type = type(plan_node1)

        if issubclass(nodes_type, TreePlanInternalNode):
            if plan_node1.operator != plan_node2.operator:
                return False
            if nodes_type == TreePlanUnaryNode:
                return UnifiedTreeBuilder.is_equivalent(plan_node1.child, pattern1, plan_node2.child, pattern2, leaves_dict)

            if nodes_type == TreePlanBinaryNode:
                return UnifiedTreeBuilder.is_equivalent(plan_node1.left_child, pattern1, plan_node2.left_child, pattern2, leaves_dict) \
                       and UnifiedTreeBuilder.is_equivalent(plan_node1.right_child, pattern1, plan_node2.right_child, pattern2, leaves_dict)

        if nodes_type == TreePlanLeafNode:
            leaf_node1 = leaves_dict.get(pattern1).get(plan_node1)
            leaf_node2 = leaves_dict.get(pattern2).get(plan_node2)
            if leaf_node1 and leaf_node2:
                return leaf_node1.get_event_name() == leaf_node2.get_event_name() and leaf_node1.is_equivalent(leaf_node2)

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
            leaves_nodes[tree_plan] = LeafNode(sliding_window, tree_plan.event_index, root_operator.args[tree_plan.event_index], None)
            return leaves_nodes
        leaves_nodes = UnifiedTreeBuilder.tree_get_leaves(root_operator, tree_plan.left_child, args, sliding_window, consumption_policy)
        leaves_nodes.update(UnifiedTreeBuilder.tree_get_leaves(root_operator, tree_plan.right_child, args, sliding_window, consumption_policy))
        return leaves_nodes

    @staticmethod
    def _tree_plans_update_leaves(pattern_to_tree_plan_map: Dict[Pattern, TreePlan], shared_leaves_dict):
        """at this function we share same leaves in different patterns to share the same root ,
        that says one instance of each unique leaf is created ,
        (if a leaf exists in two trees , the leaf will have two parents)
        """
        for pattern, tree_plan in pattern_to_tree_plan_map.items():
            updated_tree_plan_root = UnifiedTreeBuilder._single_tree_plan_update_leaves(tree_plan.root, shared_leaves_dict)
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
