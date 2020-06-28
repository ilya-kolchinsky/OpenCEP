from datetime import timedelta, datetime
from base.Pattern import Pattern
from base.PatternStructure import PatternStructure, SeqOperator, AndOperator, QItem
from misc.IOUtils import Stream
from typing import List, Tuple
from base.PatternMatch import PatternMatch
from evaluation.EvaluationMechanism import EvaluationMechanism
from evaluation.Nodes.Node import Node
from evaluation.Nodes.InternalNode import InternalNode, SeqNode, AndNode
from evaluation.Nodes.LeafNode import LeafNode
from evaluation.Storage import TreeStorageParameters


class Tree:
    """
    Represents an evaluation tree. Implements the functionality of constructing an actual tree from a "tree structure"
    object returned by a tree builder. Other than that, merely acts as a proxy to the tree root node.
    """

    def __init__(self, tree_structure: tuple, pattern: Pattern, storage_params: TreeStorageParameters):
        # Note that right now only "flat" sequence patterns and "flat" conjunction patterns are supported
        self.__root = Tree.__construct_tree(
            pattern.structure.get_top_operator() == SeqOperator, tree_structure, pattern.structure.args, pattern.window
        )
        self.__root.apply_formula(pattern.condition)
        self.__root.create_storage_unit(storage_params)

    def json_repr(self):
        return self.__root.json_repr()

    @staticmethod
    def __construct_tree(
        is_sequence: bool,
        tree_structure: tuple or int,
        args: List[QItem],
        sliding_window: timedelta,
        parent: Node = None,
    ):

        if type(tree_structure) != int and len(tree_structure) == 1:
            tree_structure = tree_structure[0]

        if type(tree_structure) == int:
            return LeafNode(sliding_window, tree_structure, args[tree_structure], parent)

        current = SeqNode(sliding_window, parent) if is_sequence else AndNode(sliding_window, parent)
        left_structure, right_structure = tree_structure

        left = Tree.__construct_tree(is_sequence, left_structure, args, sliding_window, current)
        right = Tree.__construct_tree(is_sequence, right_structure, args, sliding_window, current)
        current.set_subtrees(left, right)
        return current

    def get_leaves(self):
        return self.__root.get_leaves()

    def get_matches(self):
        while self.__root.has_partial_matches():
            yield self.__root.consume_first_partial_match().events


class TreeBasedEvaluationMechanism(EvaluationMechanism):
    """
    An implementation of the tree-based evaluation mechanism.
    """

    def __init__(self, pattern: Pattern, tree_structure: tuple, storage_params: TreeStorageParameters):
        self.__tree = Tree(tree_structure, pattern, storage_params)

    def json_repr(self):
        return self.__tree.json_repr()

    def eval(self, events: Stream, matches: Stream):
        event_types_listeners = {}
        # register leaf listeners for event types.
        for leaf in self.__tree.get_leaves():
            event_type = leaf.get_event_type()
            if event_type in event_types_listeners.keys():
                event_types_listeners[event_type].append(leaf)
            else:
                event_types_listeners[event_type] = [leaf]

        # Send events to listening leaves.
        for event in events:
            if event.event_type in event_types_listeners.keys():
                for leaf in event_types_listeners[event.event_type]:
                    leaf.handle_event(event)
                    for match in self.__tree.get_matches():
                        matches.add_item(PatternMatch(match))

        matches.close()
