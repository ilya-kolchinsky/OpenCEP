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
from test.UnitTests.prettyjson import prettyjson


class Tree:
    """
    Represents an evaluation tree. Implements the functionality of constructing an actual tree from a "tree structure"
    object returned by a tree builder. Other than that, merely acts as a proxy to the tree root node.
    """

    def __init__(self, tree_structure: tuple, pattern: Pattern):
        # Note that right now only "flat" sequence patterns and "flat" conjunction patterns are supported
        self.__root = Tree.__construct_tree(
            pattern.structure.get_top_operator() == SeqOperator,  # Currently only SeqOperator and AndOperator
            tree_structure,
            pattern.structure.args,  # if we expect * or ~ Operator then should change
            pattern.window,
        )
        # a function bdal the next two called: set_up_nodes which applies formula simplifies it and then creates suitable storage units
        self.__root.apply_formula(pattern.condition)  # puts formula in nodes
        self.__root.create_storage_unit(sorting_key=None)  # root #MUH
        # self.__root.set_sorting_properties()

    def json_repr(self):
        return self.__root.json_repr()

    @staticmethod
    def __construct_tree(
        is_sequence: bool,
        tree_structure: tuple or int,
        args: List[QItem],  # List[QItems, SeqOperators, AndOperators]
        sliding_window: timedelta,
        parent: Node = None,
    ):

        # because splitting the tuple (0,1) returns (0,) and (1,)
        if type(tree_structure) != int and len(tree_structure) == 1:
            tree_structure = tree_structure[0]
        # stop condition
        if type(tree_structure) == int:
            return LeafNode(sliding_window, tree_structure, args[tree_structure], parent)

        current = SeqNode(sliding_window, parent) if is_sequence else AndNode(sliding_window, parent)
        # OLD: left_structure, right_structure = tree_structure
        left_structure = tree_structure[: len(tree_structure) // 2]
        right_structure = tree_structure[len(tree_structure) // 2 :]
        left = Tree.__construct_tree(is_sequence, left_structure, args, sliding_window, current)
        right = Tree.__construct_tree(is_sequence, right_structure, args, sliding_window, current)
        current.set_subtrees(left, right)  # sets event_defs also
        return current

    def get_leaves(self):
        return self.__root.get_leaves()

    def get_matches(self):
        while self.__root.has_partial_matches():
            yield self.__root.consume_first_partial_match().events

    """def _create_sequence_dict(self, ps: PatternStructure):
        # event_def[1] is of type QItem
        sequence_dict = {
            event_def[1].name: set()
            for event_def in self.__root.get_event_definitions()
        }
        current_operator = ps.get_top_operator()
        if current_operator == SeqOperator:
            for i in range(len(ps.args) - 1):
                for j in range(i + 1, len(ps.args)):
                    sequence_dict[ps.args[i].name].add(ps.args[j].name)
        return sequence_dict
    """
    """ def fill_sequence_dict(ps: PatternStructure):
            current_operator = ps.get_top_operator()
            if current_operator == SeqOperator:
                for arg in ps.args
                if isinstance(arg, QItem):
                    fill_all_after(ps,arg.name)
                else:
                    fill_sequence_dict(arg)
    """


class TreeBasedEvaluationMechanism(EvaluationMechanism):
    """
    An implementation of the tree-based evaluation mechanism.
    """

    # def get_leaves(self):
    # return self.__tree.get_leaves()

    def __init__(self, pattern: Pattern, tree_structure: tuple):
        """self.__tree: Tree
        if type(tree_structure) == tuple and len(tree_structure) == 0:
            self.__tree = None
            return  # given empty tuple -> ()"""

        self.__tree = Tree(tree_structure, pattern)

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
                        matches.append(PatternMatch(match))  # TODO append -> add_item
        # maybe we should put them all at once in unhandled then after that we could call handle for some of them
        # matches.close() #TODO uncomment this line
