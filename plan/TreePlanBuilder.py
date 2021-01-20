from abc import ABC

from base.Pattern import Pattern
from base.PatternStructure import *
from plan.TreeCostModel import TreeCostModelFactory
from plan.TreeCostModels import TreeCostModels
from plan.TreePlan import TreePlan, TreePlanNode, OperatorTypes, TreePlanBinaryNode
from misc.StatisticsTypes import StatisticsTypes
from misc.Statistics import MissingStatisticsException
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
        """
        This function creates a selectivity matrix that fits the root operator (kind of flattening the selectivity
        of the nested operators, if exists).
        """
        if pattern.statistics_type != StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            raise MissingStatisticsException()
        (selectivityMatrix, _) = pattern.statistics
        if pattern.count_primitive_positive_events() != len(selectivityMatrix):
            raise Exception("size mismatch")
        nested_selectivity_matrix = []
        primitive_sons_list = []
        # event_names are all the events in this pattern (including those under nested operators)
        event_names = [name for name in pattern.positive_structure.get_primitive_events_names()]
        for arg in pattern.positive_structure.get_args():
            # This is a list with size of the number of args, where each entry in the list is the events' name of the
            # primitive events under this arg.
            primitive_sons_list.append([name for name in arg.get_primitive_events_names()])
        for i, row_entry in enumerate(primitive_sons_list):
            nested_selectivity_matrix.append([])
            for col_entry in primitive_sons_list:
                # Building the new matrix, which is (#args x #args), where each entry is calculated based on the events
                # under the specific args respectively
                nested_selectivity_matrix[i].append(TreePlanBuilder._calculate_nested_selectivity(event_names, selectivityMatrix, row_entry, col_entry))
        return nested_selectivity_matrix

    @staticmethod
    def _calculate_nested_selectivity(all_events_names, selectivity_matrix, arg1_names, arg2_names):
        """
        Calculates the selectivity of a new entry in the top (new generated) matrix, based on list of all the names in
        pattern, and the 2 args' nested primitive events, that we want to calculate their selectivity.
        We multiply all the selectivities combinations of pairs that contain one from the first arg events and one from
        the second arg events.
        """
        selectivity = 1.0
        for arg1_name in arg1_names:
            for arg2_name in arg2_names:
                selectivity = selectivity * selectivity_matrix[all_events_names.index(arg1_name)][all_events_names.index(arg2_name)]
        return selectivity

    @staticmethod
    def _chop_matrix(pattern: Pattern, arg: PatternStructure):
        """
        Chop the matrix and returns only the rows and columns that are relevant to this arg (assuming this arg is in
        pattern's operators.), based on the nested events' name in this arg.
        """
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