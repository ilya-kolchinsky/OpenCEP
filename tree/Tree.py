from datetime import timedelta
from typing import List
from copy import deepcopy

from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, AndOperator, PatternStructure, CompositeStructure, UnaryStructure, \
    KleeneClosureOperator, PrimitiveEventStructure, NegationOperator
from misc.ConsumptionPolicy import ConsumptionPolicy
from plan.TreePlan import TreePlan, TreePlanNode, TreePlanLeafNode, TreePlanBinaryNode, OperatorTypes, \
    TreePlanInternalNode
from tree.nodes.AndNode import AndNode
from tree.nodes.KleeneClosureNode import KleeneClosureNode
from tree.nodes.LeafNode import LeafNode
from tree.nodes.NegationNode import NegativeSeqNode, NegativeAndNode, NegationNode
from tree.nodes.Node import Node
from tree.PatternMatchStorage import TreeStorageParameters
from tree.nodes.SeqNode import SeqNode


class Tree:
    """
    Represents an evaluation tree. Implements the functionality of constructing an actual tree from a "tree positive_structure"
    object returned by a tree builder. Other than that, merely acts as a proxy to the tree root node.
    The pattern_id parameter is used in multi-pattern mode.
    """
    def __init__(self, tree_plan: TreePlan, pattern: Pattern, storage_params: TreeStorageParameters,
                 pattern_id: int = None):
        self.__root = self.__construct_tree(pattern.positive_structure, tree_plan.root,
                                            self.__get_operator_arg_list(pattern.full_structure),
                                            pattern.window, None, pattern.consumption_policy)

        if pattern.consumption_policy is not None and \
                pattern.consumption_policy.should_register_event_type_as_single(True):
            for event_type in pattern.consumption_policy.single_types:
                self.__root.register_single_event_type(event_type)

        self.__apply_condition(pattern)
        self.__root.create_storage_unit(storage_params)

        self.__root.create_parent_to_info_dict()
        self.__root.set_is_output_node(True)
        if pattern_id is not None:
            pattern.id = pattern_id
            self.__root.propagate_pattern_id(pattern_id)

    def __apply_condition(self, pattern: Pattern):
        """
        Applies the condition of the given pattern on the evaluation tree.
        The condition is copied since it is modified inside the recursive apply_condition call.
        """
        condition_copy = deepcopy(pattern.condition)
        self.__root.apply_condition(condition_copy)
        if condition_copy.get_num_conditions() > 0:
            raise Exception("Unused conditions after condition propagation: {}".format(
                condition_copy.get_conditions_list()))

    def get_leaves(self):
        return self.__root.get_leaves()

    def get_matches(self):
        while self.__root.has_unreported_matches():
            yield self.__root.get_next_unreported_match()

    def get_structure_summary(self):
        """
        Returns a tuple summarizing the structure of the tree.
        """
        return self.__root.get_structure_summary()

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
    def __instantiate_internal_node(operator_node: TreePlanInternalNode, sliding_window: timedelta, parent: Node):
        """
        Creates an internal node representing a given operator.
        """
        if operator_node.operator == OperatorTypes.SEQ:
            return SeqNode(sliding_window, parent)
        if operator_node.operator == OperatorTypes.AND:
            return AndNode(sliding_window, parent)
        if operator_node.operator == OperatorTypes.NSEQ:
            return NegativeSeqNode(sliding_window, operator_node.is_unbounded, parent)
        if operator_node.operator == OperatorTypes.NAND:
            return NegativeAndNode(sliding_window, operator_node.is_unbounded, parent)
        if operator_node.operator == OperatorTypes.KC:
            return KleeneClosureNode(sliding_window, operator_node.min_size, operator_node.max_size, parent)
        raise Exception("Unknown or unsupported operator %s" % (operator_node.operator,))

    def __handle_primitive_event_or_nested_structure(self, tree_plan_leaf: TreePlanLeafNode,
                                                     current_operator: PatternStructure,
                                                     sliding_window: timedelta, parent: Node,
                                                     consumption_policy: ConsumptionPolicy):
        """
        Constructs a single leaf node or a subtree with nested structure according to the input parameters.
        """
        if isinstance(current_operator, PrimitiveEventStructure):
            # the current operator is a primitive event - we should simply create a leaf
            event = current_operator
            if consumption_policy is not None and \
                    consumption_policy.should_register_event_type_as_single(False, event.type):
                parent.register_single_event_type(event.type)
            return LeafNode(sliding_window, tree_plan_leaf.event_index, event, parent)

        if isinstance(current_operator, UnaryStructure):
            # the current operator is a unary operator hiding a nested pattern structure
            unary_node = self.__instantiate_internal_node(tree_plan_leaf, sliding_window, parent)
            nested_operator = current_operator.arg
            child = self.__construct_tree(nested_operator, Tree.__create_nested_structure(nested_operator),
                                          Tree.__get_operator_arg_list(nested_operator), sliding_window, unary_node,
                                          consumption_policy)
            unary_node.set_subtree(child)
            return unary_node

        # the current operator is a nested binary operator
        return self.__construct_tree(current_operator, Tree.__create_nested_structure(current_operator),
                                     current_operator.args, sliding_window, parent, consumption_policy)

    def __construct_tree(self, root_operator: PatternStructure, tree_plan: TreePlanNode,
                         args: List[PatternStructure], sliding_window: timedelta, parent: Node,
                         consumption_policy: ConsumptionPolicy):
        """
        Recursively builds an evaluation tree according to the specified structure.
        """
        if isinstance(root_operator, UnaryStructure) and parent is None:
            # a special case where the top operator of the entire pattern is an unary operator
            return self.__handle_primitive_event_or_nested_structure(tree_plan, root_operator,
                                                                     sliding_window, parent, consumption_policy)

        if type(tree_plan) == TreePlanLeafNode:
            # either a leaf node or an unary operator encapsulating a nested structure
            # TODO: must implement a mechanism for actually creating nested tree plans instead of a flat plan
            # with leaves hiding nested structure
            return self.__handle_primitive_event_or_nested_structure(tree_plan, args[tree_plan.event_index],
                                                                     sliding_window, parent, consumption_policy)

        # an internal node
        current = self.__instantiate_internal_node(tree_plan, sliding_window, parent)
        left_subtree = self.__construct_tree(root_operator, tree_plan.left_child, args,
                                             sliding_window, current, consumption_policy)
        right_subtree = self.__construct_tree(root_operator, tree_plan.right_child, args,
                                              sliding_window, current, consumption_policy)
        current.set_subtrees(left_subtree, right_subtree)
        return current

    def get_last_matches(self):
        """
        After the system run is completed, retrieves and returns the last pending matches.
        As of now, the only case in which such matches may exist is if a pattern contains an unbounded negative event
        (e.g., SEQ(A,B,NOT(C)), in which case positive partial matches wait for timeout before proceeding to the root.
        """
        if not isinstance(self.__root, NegationNode):
            return []
        # this is the node that contains the pending matches
        first_unbounded_negative_node = self.__root.get_first_unbounded_negative_node()
        if first_unbounded_negative_node is None:
            return []
        first_unbounded_negative_node.flush_pending_matches()
        # the pending matches were released and have hopefully reached the root
        return self.get_matches()

    @staticmethod
    def __create_nested_structure(nested_operator: PatternStructure):
        """
        This method is a temporal hack, hopefully it will be removed soon.
        # TODO: calculate the evaluation order in the way it should work - using a tree plan builder
        """
        order = list(range(len(nested_operator.args))) if isinstance(nested_operator, CompositeStructure) else [0]
        operator_type = None
        if isinstance(nested_operator, AndOperator):
            operator_type = OperatorTypes.AND
        elif isinstance(nested_operator, SeqOperator):
            operator_type = OperatorTypes.SEQ
        ret = TreePlanLeafNode(order[0])
        for i in range(1, len(order)):
            ret = TreePlanBinaryNode(operator_type, ret, TreePlanLeafNode(order[i]))
        return ret

    def get_root(self):
        """
        Returns the root node of the tree.
        """
        return self.__root
