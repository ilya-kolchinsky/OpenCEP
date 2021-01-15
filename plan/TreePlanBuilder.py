from abc import ABC

from base.Pattern import Pattern
from base.PatternStructure import *
from plan.TreeCostModel import TreeCostModelFactory
from plan.TreeCostModels import TreeCostModels
from plan.TreePlan import TreePlan, TreePlanNode, OperatorTypes, TreePlanBinaryNode
from misc.StatisticsTypes import StatisticsTypes
import numpy as np


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
        root = self._create_tree_topology(pattern)
        return TreePlan(root)

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
    def _selectivity_matrix_for_nested_operators(pattern: Pattern):
        if pattern.statistics_type != StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            raise Exception("not supported")
        (selectivityMatrix, _) = pattern.statistics
        if pattern.count_primitive_positive_events() != len(selectivityMatrix):
            raise Exception("size mismatch")
        nested_selectivity_matrix = []
        primitive_sons_list = []
        event_names = [name for name in pattern.positive_structure.get_primitive_events_names()]
        for arg in pattern.positive_structure.get_args():
            primitive_sons_list.append([name for name in arg.get_primitive_events_names()])
        for i, row_entry in enumerate(primitive_sons_list):
            nested_selectivity_matrix.append([])
            for col_entry in primitive_sons_list:
                nested_selectivity_matrix[i].append(TreePlanBuilder._calculate_nested_selectivity(event_names, selectivityMatrix, row_entry, col_entry))
        return nested_selectivity_matrix

    @staticmethod
    def _calculate_nested_selectivity(names, selectivity_matrix, row_entry, col_entry):
        selectivity = 1.0
        for row_name in row_entry:
           for col_name in col_entry:
               selectivity = selectivity * selectivity_matrix[names.index(row_name)][names.index(col_name)]
        return selectivity

    @staticmethod
    def _chop_matrix(pattern: Pattern, arg: PatternStructure):
        if pattern.statistics_type != StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            raise Exception("not supported")
        (selectivityMatrix, _) = pattern.statistics
        if pattern.count_primitive_positive_events() != len(selectivityMatrix):
            raise Exception("size mismatch")
        event_names = [name for name in pattern.positive_structure.get_primitive_events_names()]
        primitive_sons = [name for name in arg.get_primitive_events_names()]
        chop_start = event_names.index(primitive_sons[0])
        chop_end = event_names.index(primitive_sons[-1])
        selectivity_array = np.array(selectivityMatrix)
        return selectivity_array[chop_start:chop_end+1, chop_start:chop_end+1].tolist()