from abc import ABC
from copy import deepcopy
from typing import Dict, List

from base.Pattern import Pattern
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure
from condition.Condition import Variable
from misc import DefaultConfig
from plan.TreeCostModel import TreeCostModelFactory
from plan.TreeCostModels import TreeCostModels
from plan.TreePlan import TreePlan, TreePlanNode, OperatorTypes, TreePlanBinaryNode, TreePlanInternalNode, \
    TreePlanUnaryNode, TreePlanLeafNode


class TreePlanBuilder(ABC):
    """
    The base class for the builders of tree-based plans.
    """
    def __init__(self, cost_model_type: TreeCostModels):
        self.__cost_model = TreeCostModelFactory.create_cost_model(cost_model_type)

    def build_tree_plan(self, pattern: Pattern):
        """
        Creates a tree-based evaluation plan for the given pattern.
        """
        return TreePlan(self._create_tree_topology(pattern))

    def _create_tree_topology(self, pattern: Pattern):
        """
        An abstract method for creating the actual tree topology.
        """
        raise NotImplementedError()

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

    @staticmethod
    def replace_name_by_type_condition(first_condition, second_condition, first_pattern_events: List[PrimitiveEventStructure],
                                       second_pattern_events: List[PrimitiveEventStructure]):

        # replace every event's name in condition by his type in order to compare between condition.
        first_events_name_type = {event.name: event.type for event in first_pattern_events}
        second_events_name_type = {event.name: event.type for event in second_pattern_events}

        for e in first_condition.get_conditions_list():
            if type(e.left_term_repr) == Variable:
                e.left_term_repr.name = first_events_name_type[e.left_term_repr.name]
            if type(e.right_term_repr) == Variable:
                e.right_term_repr.name = first_events_name_type[e.right_term_repr.name]
        for e in second_condition.get_conditions_list():
            if type(e.left_term_repr) == Variable:
                e.left_term_repr.name = second_events_name_type[e.left_term_repr.name]
            if type(e.right_term_repr) == Variable:
                e.right_term_repr.name = second_events_name_type[e.right_term_repr.name]

        return first_condition, second_condition

    @staticmethod
    def extract_pattern_condition(plan_node, pattern, leaves_dict):
        leaves_in_plan_node = plan_node.get_leaves()
        if leaves_in_plan_node is None:
            return None

        pattern_leaves, pattern_events = list(zip(*list(leaves_dict.get(pattern).items())))

        event_indexes = list(map(lambda e: e.event_index, leaves_in_plan_node))
        plan_node_events = list(
            filter(lambda i: pattern_leaves[i].event_index in event_indexes, range(len(pattern_leaves))))

        names = {pattern_events[event_index].name for event_index in plan_node_events}
        return names, pattern_events, event_indexes

    @staticmethod
    def get_condition_from_pattern_in_sub_tree(first_plan_node: TreePlanNode, first_pattern: Pattern, second_plan_node: TreePlanNode,
                                               second_pattern: Pattern,
                                               leaves_dict):

        first_names, first_pattern_events, _ = TreePlanBuilder.extract_pattern_condition(first_plan_node, first_pattern, leaves_dict)
        second_names, second_pattern_events, _ = TreePlanBuilder.extract_pattern_condition(second_plan_node, second_pattern, leaves_dict)
        first_condition = deepcopy(first_pattern.condition.get_condition_of(first_names, get_kleene_closure_conditions=False,
                                                                       consume_returned_conditions=False))
        second_condition = deepcopy(second_pattern.condition.get_condition_of(second_names, get_kleene_closure_conditions=False,
                                                                        consume_returned_conditions=False))

        if first_condition == second_condition:
            return first_condition, second_condition
        return TreePlanBuilder.replace_name_by_type_condition(first_condition, second_condition, first_pattern_events,
                                                              second_pattern_events)

    @staticmethod
    def is_equivalent(first_plan_node: TreePlanNode, first_pattern: Pattern, second_plan_node: TreePlanNode, second_pattern: Pattern,
                      leaves_dict: Dict[Pattern, Dict[TreePlanNode, PrimitiveEventStructure]]):

        """
        find if two subtree_plans are euivalent and check that by recursion on left subtree_plans and right subtree_plans
        the way this function works is comparing one node in pattern1 with its corresponding node in pattern2 , in addition to comparing
        the hierarchy we compare the conditions too, if we counter two nodes with different condition set or different subtrees hierarchy
        , we return false
        """
        if type(first_plan_node) != type(second_plan_node) or first_plan_node is None or second_plan_node is None:
            return False
        # we have to extract both condition lists since it's not possible to implement this function using __eq__
        # hierarchy because the input is not and instance of this class .
        first_condition, second_condition = TreePlanBuilder.get_condition_from_pattern_in_sub_tree(first_plan_node, first_pattern,
                                                                                                   second_plan_node, second_pattern,
                                                                                                   leaves_dict)

        if first_condition is None or second_condition is None or first_condition != second_condition:
            return False

        nodes_type = type(first_plan_node)

        if issubclass(nodes_type, TreePlanInternalNode):
            if first_plan_node.operator != second_plan_node.operator:
                return False
            if nodes_type == TreePlanUnaryNode:
                return TreePlanBuilder.is_equivalent(first_plan_node.child, first_pattern, second_plan_node.child, second_pattern,
                                                     leaves_dict)

            if nodes_type == TreePlanBinaryNode:
                return TreePlanBuilder.is_equivalent(first_plan_node.left_child, first_pattern, second_plan_node.left_child,
                                                     second_pattern, leaves_dict) \
                       and TreePlanBuilder.is_equivalent(first_plan_node.right_child, first_pattern, second_plan_node.right_child,
                                                         second_pattern, leaves_dict)

        if nodes_type == TreePlanLeafNode:
            first_event = leaves_dict.get(first_pattern).get(first_plan_node)
            second_event = leaves_dict.get(second_pattern).get(second_plan_node)
            if first_event and second_event:
                return first_event.type == second_event.type

        return False
