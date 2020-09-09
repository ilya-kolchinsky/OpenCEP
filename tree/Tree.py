from datetime import timedelta
from typing import Dict, List

from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, AndOperator, PatternStructure, CompositeStructure, UnaryStructure, \
    KleeneClosureOperator, PrimitiveEventStructure, NegationOperator
from misc.ConsumptionPolicy import ConsumptionPolicy
from tree.AndNode import AndNode
from tree.KleeneClosureNode import KleeneClosureNode
from tree.LeafNode import LeafNode
from tree.NegationNode import NegativeSeqNode, NegativeAndNode, NegationNode
from tree.Node import Node
from tree.PatternMatchStorage import TreeStorageParameters
from tree.SeqNode import SeqNode


class Tree:
    """
    Represents an evaluation tree. Implements the functionality of constructing an actual tree from a "tree positive_structure"
    object returned by a tree builder. Other than that, merely acts as a proxy to the tree root node.
    """
    def __init__(self, tree_structure: tuple, pattern: Pattern, storage_params: TreeStorageParameters):
        self.__root = self.__construct_tree(pattern.positive_structure, tree_structure,
                                            Tree.__get_operator_arg_list(pattern.positive_structure),
                                            pattern.window, None, pattern.consumption_policy)

        if pattern.consumption_policy is not None and \
                pattern.consumption_policy.should_register_event_type_as_single(True):
            for event_type in pattern.consumption_policy.single_types:
                self.__root.register_single_event_type(event_type)

        if pattern.negative_structure is not None:
            self.__adjust_leaf_indices(pattern)
            self.__add_negative_tree_structure(pattern)

        self.__root.apply_formula(pattern.condition)
        self.__root.create_storage_unit(storage_params)

    def __adjust_leaf_indices(self, pattern: Pattern):
        """
        Fixes the values of the leaf indices in the positive tree to take the negative events into account.
        """
        leaf_mapping = {}
        for leaf in self.get_leaves():
            current_index = leaf.get_leaf_index()
            correct_index = pattern.get_index_by_event_name(leaf.get_event_name())
            leaf_mapping[current_index] = correct_index
        self.__update_event_defs(self.__root, leaf_mapping)

    def __update_event_defs(self, node: Node, leaf_mapping: Dict[int, int]):
        """
        Recursively modifies the event indices in the tree specified by the given node.
        """
        if isinstance(node, LeafNode):
            node.set_leaf_index(leaf_mapping[node.get_leaf_index()])
            return
        # this node is an internal node
        event_defs = node.get_event_definitions()
        # no list comprehension is used since we modify the original list
        for i in range(len(event_defs)):
            event_def = event_defs[i]
            event_defs[i] = (leaf_mapping[event_def[0]], event_def[1])
        self.__update_event_defs(node.get_left_subtree(), leaf_mapping)
        self.__update_event_defs(node.get_right_subtree(), leaf_mapping)

    def __add_negative_tree_structure(self, pattern: Pattern):
        """
        Adds the negative nodes at the root of the tree.
        """
        top_operator = pattern.full_structure.get_top_operator()
        negative_event_list = pattern.negative_structure.get_args()
        current_root = self.__root
        for negation_operator in negative_event_list:
            if top_operator == SeqOperator:
                new_root = NegativeSeqNode(pattern.window,
                                           is_unbounded=Tree.__is_unbounded_negative_event(pattern, negation_operator))
            elif top_operator == AndOperator:
                new_root = NegativeAndNode(pattern.window,
                                           is_unbounded=Tree.__is_unbounded_negative_event(pattern, negation_operator))
            else:
                raise Exception("Unsupported operator for negation: %s" % (top_operator,))
            negative_event = negation_operator.arg
            leaf_index = pattern.get_index_by_event_name(negative_event.name)
            negative_leaf = LeafNode(pattern.window, leaf_index, negative_event, new_root)
            new_root.set_subtrees(current_root, negative_leaf)
            negative_leaf.set_parent(new_root)
            current_root.set_parent(new_root)
            current_root = new_root
        self.__root = current_root

    def get_leaves(self):
        return self.__root.get_leaves()

    def get_matches(self):
        while self.__root.has_partial_matches():
            yield self.__root.consume_first_partial_match()

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
    def __create_internal_node_by_operator(operator: PatternStructure, sliding_window: timedelta, parent: Node = None):
        """
        Creates an internal node representing a given operator.
        Note that negation node types are intentionally not supported here since the negative part of a pattern is
        added in a separate construction stage.
        """
        operator_type = operator.get_top_operator()
        if operator_type == SeqOperator:
            return SeqNode(sliding_window, parent)
        if operator_type == AndOperator:
            return AndNode(sliding_window, parent)
        if operator_type == KleeneClosureOperator:
            return KleeneClosureNode(sliding_window, operator.min_size, operator.max_size, parent)
        raise Exception("Unknown or unsupported operator %s" % (operator_type,))

    def __handle_primitive_event_or_nested_structure(self, event_index: int, current_operator: PatternStructure,
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
            return LeafNode(sliding_window, event_index, current_operator, parent)

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

    def __construct_tree(self, root_operator: PatternStructure, tree_structure: tuple or int,
                         args: List[PatternStructure], sliding_window: timedelta, parent: Node,
                         consumption_policy: ConsumptionPolicy):
        """
        Recursively builds an evaluation tree according to the specified structure.
        """
        if isinstance(root_operator, UnaryStructure) and parent is None:
            # a special case where the top operator of the entire pattern is an unary operator
            return self.__handle_primitive_event_or_nested_structure(tree_structure, root_operator,
                                                                     sliding_window, parent, consumption_policy)

        if type(tree_structure) == int:
            # either a leaf node or an unary operator encapsulating a nested structure
            return self.__handle_primitive_event_or_nested_structure(tree_structure, args[tree_structure],
                                                                     sliding_window, parent, consumption_policy)

        # an internal node
        current = self.__create_internal_node_by_operator(root_operator, sliding_window, parent)
        left_structure, right_structure = tree_structure
        left = self.__construct_tree(root_operator, left_structure, args, sliding_window, current, consumption_policy)
        right = self.__construct_tree(root_operator, right_structure, args, sliding_window, current, consumption_policy)
        current.set_subtrees(left, right)
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
    def __create_nested_structure(nested_operator):
        """
        This method is a temporal hack, hopefully it will be removed soon.
        # TODO: calculate the evaluation order in the way it should work - using a tree plan builder
        """
        order = list(range(len(nested_operator.args))) if isinstance(nested_operator, CompositeStructure) else [0]
        ret = order[0]
        for i in range(1, len(order)):
            ret = (ret, order[i])
        return ret

    @staticmethod
    def __is_unbounded_negative_event(pattern: Pattern, negation_operator: NegationOperator):
        """
        Returns True if the negative event represented by the given operator is unbounded (i.e., can appear after the
        entire match is ready and invalidate it) and False otherwise.
        """
        if pattern.full_structure.get_top_operator() != SeqOperator:
            return True
        # for a sequence pattern, a negative event is unbounded if no positive events follow it
        # the implementation below assumes a flat sequence
        sequence_elements = pattern.full_structure.get_args()
        operator_index = sequence_elements.index(negation_operator)
        for i in range(operator_index + 1, len(sequence_elements)):
            if isinstance(sequence_elements[i], PrimitiveEventStructure):
                return False
        return True
