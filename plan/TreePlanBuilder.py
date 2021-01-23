from abc import ABC
from copy import deepcopy
from typing import Dict, List

from base.Pattern import Pattern
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure
from condition.Condition import Variable
from misc import DefaultConfig
from plan.TreeCostModel import TreeCostModelFactory
from plan.TreeCostModels import TreeCostModels
from plan.TreePlan import TreePlan, TreePlanNode, OperatorTypes, TreePlanBinaryNode, TreePlanInternalNode, TreePlanUnaryNode, TreePlanLeafNode
from tree.TreeVisualizationUtility import GraphVisualization


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

        # replace every event's name in condition by his type in opder to compare between condition.
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
    def get_condition_from_pattern_in_sub_tree(plan_node1: TreePlanNode, pattern1: Pattern, plan_node2: TreePlanNode,
                                               pattern2: Pattern,
                                               leaves_dict):

        leaves_in_plan_node_1 = plan_node1.get_leaves()
        leaves_in_plan_node_2 = plan_node2.get_leaves()
        if leaves_in_plan_node_1 is None or leaves_in_plan_node_2 is None:
            return None, None

        pattern1_leaves, pattern1_events = list(zip(*list(leaves_dict.get(pattern1).items())))
        pattern2_leaves, pattern2_events = list(zip(*list(leaves_dict.get(pattern2).items())))

        event_indexes1 = list(map(lambda e: e.event_index, leaves_in_plan_node_1))
        plan_node_1_events = list(
            filter(lambda i: pattern1_leaves[i].event_index in event_indexes1, range(len(pattern1_leaves))))

        event_indexes2 = list(map(lambda e: e.event_index, leaves_in_plan_node_2))
        plan_node_2_events = list(
            filter(lambda i: pattern2_leaves[i].event_index in event_indexes2, range(len(pattern2_leaves))))

        names1 = {pattern1_events[event_index].name for event_index in plan_node_1_events}
        names2 = {pattern2_events[event_index].name for event_index in plan_node_2_events}

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
        """
        if type(plan_node1) != type(plan_node2) or plan_node1 is None or plan_node2 is None:
            return False

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
