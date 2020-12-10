from datetime import timedelta
from typing import List
from copy import deepcopy

from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, AndOperator, PatternStructure, CompositeStructure, UnaryStructure, \
    KleeneClosureOperator, PrimitiveEventStructure, NegationOperator
from misc.ConsumptionPolicy import ConsumptionPolicy
from plan.TreePlan import TreePlan, TreePlanNode, TreePlanLeafNode, TreePlanBinaryNode, OperatorTypes
from tree.nodes.AndNode import AndNode
from tree.nodes.KleeneClosureNode import KleeneClosureNode
from tree.nodes.LeafNode import LeafNode
from tree.nodes.NegationNode import NegativeSeqNode, NegativeAndNode, NegationNode
from tree.nodes.Node import Node, PrimitiveEventDefinition
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
        args = pattern.positive_structure.get_args() if isinstance(pattern.positive_structure, CompositeStructure)\
            else [pattern.positive_structure.get_arg()]
        if pattern.negative_structure is not None:
            args.extend(pattern.negative_structure.get_args())
        self.__root = self.__construct_tree(pattern.positive_structure, tree_plan.root, args,
                                            pattern.window, None, pattern.consumption_policy)

        if pattern.consumption_policy is not None and \
                pattern.consumption_policy.should_register_event_type_as_single(True):
            for event_type in pattern.consumption_policy.single_types:
                self.__root.register_single_event_type(event_type)

        if pattern.negative_structure is not None:
            self.__adjust_leaf_indices(pattern)
            self.__update_bounded_neg_seq_nodes(pattern)

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

    def __adjust_leaf_indices(self, pattern: Pattern):
        """
        Fixes the values of the leaf indices in the positive tree to take the negative events into account.
        """
        leaf_mapping = {}
        # update the leaves
        for leaf in self.get_leaves():
            current_index = leaf.get_leaf_index()
            correct_index = pattern.get_index_by_event_name(leaf.get_event_name())
            leaf.set_leaf_index(correct_index)
            leaf_mapping[current_index] = correct_index
        # update the event definitions in the internal nodes
        # note that it is enough to only update the root since it contains all the event definition objects
        for event_def in self.__root.get_positive_event_definitions():
            event_def.index = leaf_mapping[event_def.index]
        current = self.__root
        while isinstance(current, NegationNode):
            current.get_negative_event_defs()[0].index = leaf_mapping[current.get_negative_event_defs()[0].index]
            current.get_event_definitions().sort(key=PrimitiveEventDefinition.get_event_index)
            current = current.get_left_subtree()

    def __update_bounded_neg_seq_nodes(self, pattern: Pattern):
        # When top operator is And, negations are unbounded anyway
        if pattern.full_structure.get_top_operator() == AndOperator:
            return
        current_node = self.__root
        while isinstance(current_node, NegativeSeqNode):
            current_node.set_is_unbounded(Tree.__is_unbounded_negative_event(pattern, current_node.get_right_subtree().
                                                                             get_leaves()[0].get_leaf_index()))
            current_node = current_node.get_left_subtree()

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
    def __create_internal_node_by_operator(operator: PatternStructure, sliding_window: timedelta,
                                           parent: Node = None, flag: bool = False):
        """
        Creates an internal node representing a given operator.
        In case of negation node, the flag parameter would be "True", and according to the operator parameter,
        the function would set the internal node to be NegativeSeqNode or NegativeAndNode.
        The is_undounded parameter passed to the Negative nodes set to true (and will be updated afterwards).
        """
        operator_type = operator.get_top_operator()
        if operator_type == SeqOperator:
            if flag:
                return NegativeSeqNode(sliding_window, flag, parent)
            return SeqNode(sliding_window, parent)
        if operator_type == AndOperator:
            if flag:
                return NegativeAndNode(sliding_window, flag, parent)
            return AndNode(sliding_window, parent)
        if operator_type == KleeneClosureOperator:
            return KleeneClosureNode(sliding_window, operator.min_size, operator.max_size, parent)
        raise Exception("Unknown or unsupported operator %s" % (operator_type,))

    def __handle_primitive_event_or_nested_structure(self, tree_plan_leaf: TreePlanLeafNode,
                                                     current_operator: PatternStructure,
                                                     sliding_window: timedelta, parent: Node,
                                                     consumption_policy: ConsumptionPolicy):
        """
        Constructs a single leaf node or a subtree with nested structure according to the input parameters.
        """
        if isinstance(current_operator, NegationOperator):
            current_operator = current_operator.arg
        if isinstance(current_operator, PrimitiveEventStructure):
            # the current operator is a primitive event - we should simply create a leaf
            event = current_operator
            if consumption_policy is not None and \
                    consumption_policy.should_register_event_type_as_single(False, event.type):
                parent.register_single_event_type(event.type)
            return LeafNode(sliding_window, tree_plan_leaf.event_index, event, parent)

        if isinstance(current_operator, UnaryStructure):
            # the current operator is a unary operator hiding a nested pattern structure
            unary_node = self.__create_internal_node_by_operator(current_operator, sliding_window, parent)
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
        # In case of negation operator the flag parameter is set to true
        flag = False
        if (tree_plan.operator.name == 'NSEQ') or (tree_plan.operator.name == 'NAND'):
            flag = True
        # an internal node
        current = self.__create_internal_node_by_operator(root_operator, sliding_window, parent, flag)
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

    @staticmethod
    def __is_unbounded_negative_event(pattern: Pattern, negation_operator_index: int):
        """
        Returns True if the negative event represented by the given operator is unbounded (i.e., can appear after the
        entire match is ready and invalidate it) and False otherwise.
        """
        if pattern.full_structure.get_top_operator() != SeqOperator:
            return True
        # for a sequence pattern, a negative event is unbounded if no positive events follow it
        # the implementation below assumes a flat sequence
        sequence_elements = pattern.full_structure.get_args()
        for i in range(negation_operator_index + 1, len(sequence_elements)):
            if isinstance(sequence_elements[i], PrimitiveEventStructure):
                return False
        return True

    def get_root(self):
        """
        Returns the root node of the tree.
        """
        return self.__root
