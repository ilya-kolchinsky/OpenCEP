from abc import ABC
from datetime import timedelta
from typing import Dict, List

from base.Pattern import Pattern
from base.PatternStructure import AndOperator, SeqOperator, PatternStructure, UnaryStructure, PrimitiveEventStructure, \
    CompositeStructure
from misc import DefaultConfig
from misc.ConsumptionPolicy import ConsumptionPolicy
from plan.TreeCostModel import TreeCostModelFactory
from plan.TreeCostModels import TreeCostModels
from plan.TreePlan import TreePlan, TreePlanNode, OperatorTypes, TreePlanBinaryNode, TreePlanLeafNode, \
    TreePlanUnaryNode, TreePlanInternalNode
from plan.multi.MultiPatternUnifiedTreePlanApproaches import MultiPatternTreePlanUnionApproaches
from tree.TreeVisualizationUtility import GraphVisualization
from tree.nodes.LeafNode import LeafNode
from tree.nodes.Node import Node, PrimitiveEventDefinition


class TreePlanBuilder(ABC):
    """
    The base class for the builders of tree-based plans.
    """

    def __init__(self, cost_model_type: TreeCostModels):
        self.__cost_model = TreeCostModelFactory.create_cost_model(cost_model_type)
        self.trees_number_nodes_shared = 0

    def build_tree_plan(self, pattern: Pattern):
        """
        Creates a tree-based evaluation plan for the given pattern.
        """
        return TreePlan(self._create_tree_topology(pattern))

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
        An abstract method for creating the actual tree topology.
        """
        raise NotImplementedError()

    def _union_tree_plans(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan,
                          tree_plan_union_approach: MultiPatternTreePlanUnionApproaches):
        if isinstance(pattern_to_tree_plan_map, TreePlan) or len(pattern_to_tree_plan_map) == 1:
            return pattern_to_tree_plan_map
        if tree_plan_union_approach == MultiPatternTreePlanUnionApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES:
            return self.__share_leaves(pattern_to_tree_plan_map)
        if tree_plan_union_approach == MultiPatternTreePlanUnionApproaches.TREE_PLAN_SUBTREES_UNION:
            return self.__construct_subtrees_union_tree_plan(pattern_to_tree_plan_map)
        else:
            raise Exception("Unsupported union algorithm, yet")

    def _get_plan_cost(self, pattern: Pattern, plan: TreePlanNode):
        """
        Returns the cost of a given plan for the given plan according to a predefined cost model.
        """
        return self.__cost_model.get_plan_cost(pattern, plan)

    @staticmethod
    def _instantiate_binary_node(pattern: Pattern, left_subtree: TreePlanNode, right_subtree: TreePlanNode):
        """
        A helper method for the subclasses to instantiate tree plan nodes depending on the operator.
        """
        pattern_structure = pattern.positive_structure
        if isinstance(pattern_structure, AndOperator):
            operator_type = OperatorTypes.AND
        elif isinstance(pattern_structure, SeqOperator):
            operator_type = OperatorTypes.SEQ
        else:
            raise Exception("Unsupported binary operator")
        return TreePlanBinaryNode(operator_type, left_subtree, right_subtree)

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
            # try left merge
            return self.__try_to_share_and_merge_nodes_binary(root, root_pattern, node, node_pattern)

        if isinstance(node, TreePlanUnaryNode):
            updated_root, number_merged = self.__try_to_share_and_merge_nodes(root, root_pattern, node.child, node_pattern)
            if number_merged:
                return updated_root, number_merged

        return root, 0

    def __try_to_share_and_merge_nodes_binary(self, root, root_pattern: Pattern, node, node_pattern: Pattern):
        # try left merge
        left_side_new_root, number_merged_left = self.__try_to_share_and_merge_nodes(root, root_pattern, node.left_child, node_pattern)
        if number_merged_left:
            # right_side_new_root, number_merged_right = self.__try_to_share_and_merge_nodes(root, root_pattern, node.right_child, node_pattern)
            # if number_merged_right:
            #     return right_side_new_root, number_merged_left + number_merged_right
            return left_side_new_root, number_merged_left

        # try right merge
        right_side_new_root, number_merged_right = self.__try_to_share_and_merge_nodes(root, root_pattern, node.right_child, node_pattern)
        if number_merged_right:
            # left_side_new_root, number_merged_left = self.__try_to_share_and_merge_nodes(root, root_pattern, node.left_child, node_pattern)
            # if number_merged_left:
            #     return left_side_new_root, number_merged_left + number_merged_right
            return right_side_new_root, number_merged_right

        return root, 0

    @staticmethod
    def __sub_tree_size(root):

        if root is None:
            return 0

        if isinstance(root, TreePlanLeafNode):
            return 1

        if isinstance(root, TreePlanBinaryNode):
            return 1 + TreePlanBuilder.__sub_tree_size(root.left_child) + TreePlanBuilder.__sub_tree_size(root.right_child)

        if isinstance(root, TreePlanUnaryNode):
            return 1 + TreePlanBuilder.__sub_tree_size(root.child)

        raise Exception("Unsupported tree plan node type")

    def __find_and_merge_node_into_subtree(self, root: TreePlanNode, root_pattern: Pattern, node: TreePlanNode, node_pattern: Pattern):
        """
               This method is trying to find node in the subtree of root (or an equivalent node).
               If such a node is found, it merges the equivalent nodes.
        """
        if TreePlanBuilder.is_equivalent(root, root_pattern, node, node_pattern, self.leaves_dict):
            return node, TreePlanBuilder.__sub_tree_size(node)

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

    def __construct_subtrees_union_tree_plan(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan] or TreePlan):
        """
        This method gets patterns, builds a single-pattern tree to each one of them,
        and merges equivalent subtrees from different trees.
        We are sharing the maximal subtree that exists in the tree.
        We are assuming that each pattern appears only once in patterns (which is a legitimate assumption).
        """
        leaves_dict = {}
        for i, pattern in enumerate(pattern_to_tree_plan_map):
            tree_plan = pattern_to_tree_plan_map[pattern]
            leaves_dict[pattern] = TreePlanBuilder.tree_get_leaves(pattern.positive_structure, tree_plan.root,
                                                                   TreePlanBuilder.__get_operator_arg_list(pattern.positive_structure),
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
        leaves_dict = {}
        first_pattern = list(pattern_to_tree_plan_map.keys())[0]
        for i, pattern in enumerate(pattern_to_tree_plan_map):
            tree_plan = pattern_to_tree_plan_map[pattern]
            get_args = TreePlanBuilder.__get_operator_arg_list
            leaves_dict[pattern] = TreePlanBuilder.tree_get_leaves(pattern.positive_structure, tree_plan.root,
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

                    condition1, condition2 = TreePlanBuilder.get_condition_from_pattern_in_sub_tree(curr_leaf, curr_leaf_pattern, leaf, leaf_pattern,
                                                                                                    leaves_dict)

                    if condition1 == condition2 and curr_tree_node.get_event_name() == leaf_tree_node.get_event_name() \
                            and curr_tree_node.is_equivalent(leaf_tree_node):
                        self.trees_number_nodes_shared += 1
                        _, leaf_tree_plan_node = shared_leaves_dict[leaf]
                        break
                shared_leaves_dict[curr_leaf] = [pattern, leaf_tree_plan_node]
        return TreePlanBuilder._tree_plans_update_leaves(pattern_to_tree_plan_map,
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

        condition1, condition2 = TreePlanBuilder.get_condition_from_pattern_in_sub_tree(plan_node1, pattern1, plan_node2, pattern2, leaves_dict)

        if condition1 != condition2:
            return False

        nodes_type = type(plan_node1)

        if issubclass(nodes_type, TreePlanInternalNode):
            if plan_node1.operator != plan_node2.operator:
                return False
            if nodes_type == TreePlanUnaryNode:
                return TreePlanBuilder.is_equivalent(plan_node1.child, pattern1, plan_node2.child, pattern2, leaves_dict)

            if nodes_type == TreePlanBinaryNode:
                return TreePlanBuilder.is_equivalent(plan_node1.left_child, pattern1, plan_node2.left_child, pattern2, leaves_dict) \
                       and TreePlanBuilder.is_equivalent(plan_node1.right_child, pattern1, plan_node2.right_child, pattern2, leaves_dict)

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
        leaves_nodes = TreePlanBuilder.tree_get_leaves(root_operator, tree_plan.left_child, args, sliding_window, consumption_policy)
        leaves_nodes.update(TreePlanBuilder.tree_get_leaves(root_operator, tree_plan.right_child, args, sliding_window, consumption_policy))
        return leaves_nodes

    @staticmethod
    def _tree_plans_update_leaves(pattern_to_tree_plan_map: Dict[Pattern, TreePlan], shared_leaves_dict):
        """at this function we share same leaves in different patterns to share the same root ,
        that says one instance of each unique leaf is created ,
        (if a leaf exists in two trees , the leaf will have two parents)
        """
        for pattern, tree_plan in pattern_to_tree_plan_map.items():
            updated_tree_plan_root = TreePlanBuilder._single_tree_plan_update_leaves(tree_plan.root, shared_leaves_dict)
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
            updated_child = TreePlanBuilder._single_tree_plan_update_leaves(tree_plan_root_node.child,
                                                                            shared_leaves_dict)
            tree_plan_root_node.child = updated_child
            return tree_plan_root_node

        if type(tree_plan_root_node) == TreePlanBinaryNode:
            updated_left_child = TreePlanBuilder._single_tree_plan_update_leaves(tree_plan_root_node.left_child,
                                                                                 shared_leaves_dict)
            updated_right_child = TreePlanBuilder._single_tree_plan_update_leaves(tree_plan_root_node.right_child,
                                                                                  shared_leaves_dict)
            tree_plan_root_node.left_child = updated_left_child
            tree_plan_root_node.right_child = updated_right_child
            return tree_plan_root_node

        else:
            raise Exception("Unsupported Node type")
