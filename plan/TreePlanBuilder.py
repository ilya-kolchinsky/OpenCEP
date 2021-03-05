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

    def __init__(self, cost_model_type: TreeCostModels = DefaultConfig.DEFAULT_TREE_COST_MODEL):
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
    def replace_name_by_type_condition(condition1, condition2, pattern1_events: List[PrimitiveEventStructure],
                                       pattern2_events: List[PrimitiveEventStructure]):

        # replace every event's name in condition by his type in order to compare between condition.
        event1_name_type = {event.name: event.type for event in pattern1_events}
        event2_name_type = {event.name: event.type for event in pattern2_events}

        for e in condition1.get_conditions_list():
            if type(e.left_term_repr) == Variable:
                e.left_term_repr.name = event1_name_type[e.left_term_repr.name]
            if type(e.right_term_repr) == Variable:
                e.right_term_repr.name = event1_name_type[e.right_term_repr.name]
        for e in condition2.get_conditions_list():
            if type(e.left_term_repr) == Variable:
                e.left_term_repr.name = event2_name_type[e.left_term_repr.name]
            if type(e.right_term_repr) == Variable:
                e.right_term_repr.name = event2_name_type[e.right_term_repr.name]

        return condition1, condition2

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
    def get_condition_from_pattern_in_sub_tree(plan_node1: TreePlanNode, pattern1: Pattern, plan_node2: TreePlanNode,
                                               pattern2: Pattern,
                                               leaves_dict):

        names1, pattern1_events, _ = TreePlanBuilder.extract_pattern_condition(plan_node1, pattern1, leaves_dict)
        names2, pattern2_events, _ = TreePlanBuilder.extract_pattern_condition(plan_node2, pattern2, leaves_dict)
        condition1 = deepcopy(pattern1.condition.get_condition_of(names1, get_kleene_closure_conditions=False,
                                                                  consume_returned_conditions=False))
        condition2 = deepcopy(pattern2.condition.get_condition_of(names2, get_kleene_closure_conditions=False,
                                                                  consume_returned_conditions=False))

        if condition1 == condition2:
            return condition1, condition2
        return TreePlanBuilder.replace_name_by_type_condition(condition1, condition2, pattern1_events,
                                                              pattern2_events)

    @staticmethod
    def is_equivalent(plan_node1: TreePlanNode, pattern1: Pattern, plan_node2: TreePlanNode, pattern2: Pattern,
                      leaves_dict: Dict[Pattern, Dict[TreePlanNode, PrimitiveEventStructure]]):

        """
        find if two subtree_plans are euivalent and check that by recursion on left subtree_plans and right subtree_plans
        the way this function works is comparing one node in pattern1 with its corresponding node in pattern2 , in addition to comparing
        the hierarchy we compare the conditions too, if we counter two nodes with different condition set or different subtrees hierarchy
        , we return false
        """
        if type(plan_node1) != type(plan_node2) or plan_node1 is None or plan_node2 is None:
            return False
        # we have to extract both condition lists since it's not possible to implement this function using __eq__
        # hierarchy because the input is not and instance of this class .
        condition1, condition2 = TreePlanBuilder.get_condition_from_pattern_in_sub_tree(plan_node1, pattern1,
                                                                                        plan_node2, pattern2,
                                                                                        leaves_dict)

        if condition1 is None or condition2 is None or condition1 != condition2:
            return False

        nodes_type = type(plan_node1)

        if issubclass(nodes_type, TreePlanInternalNode):
            if plan_node1.operator != plan_node2.operator:
                return False
            if nodes_type == TreePlanUnaryNode:
                return TreePlanBuilder.is_equivalent(plan_node1.child, pattern1, plan_node2.child, pattern2,
                                                     leaves_dict)

            if nodes_type == TreePlanBinaryNode:
                return TreePlanBuilder.is_equivalent(plan_node1.left_child, pattern1, plan_node2.left_child,
                                                     pattern2, leaves_dict) \
                       and TreePlanBuilder.is_equivalent(plan_node1.right_child, pattern1, plan_node2.right_child,
                                                         pattern2, leaves_dict)

        if nodes_type == TreePlanLeafNode:
            event1 = leaves_dict.get(pattern1).get(plan_node1)
            event2 = leaves_dict.get(pattern2).get(plan_node2)
            if event1 and event2:
                return event1.type == event2.type

        return False
