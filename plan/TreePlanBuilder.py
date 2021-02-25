from abc import ABC

from base.Pattern import Pattern
from base.PatternStructure import *
from plan.TreeCostModel import TreeCostModelFactory, IntermediateResultsTreeCostModel
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
        return TreePlanBuilder._chop_matrix_aux(selectivityMatrix, chop_start, chop_end)

    @staticmethod
    def _chop_matrix_aux(matrix, start, end):
        chopped_matrix = []
        for row in range(start, end+1):
            chopped_row = []
            for index in range(start, end+1):
                chopped_row.append(matrix[row][index])
            chopped_matrix.append(chopped_row)
        return chopped_matrix

    def extract_nested_pattern(self, pattern):
        """
        TODO: UPDATE COMMENT
        This function is done recursively, to support nested pattern's operators (i.e. And(Seq,Seq)).
        When encounters KleeneClosure or CompositeStructure, it computes this operator's tree plan (recursively...)
        and uses the returned tree plan's root as a new "simple" (primitive) event (with its statistics updated
        according to its subtree nodes), such that all nested Kleene/Composite operators can be treated as "simple"
        ones.
        For every "flat" pattern, it invokes an algorithm (to be implemented by subclasses) that builds an evaluation
        order of the operands, and converts it into a left-deep tree topology.
        """
        nested_topologies = None
        nested_args = None
        nested_cost = None
        if not isinstance(pattern.positive_structure, PrimitiveEventStructure):
            # If this is not primitive, than it's a nested one.
            nested_topologies = []
            nested_args = []
            nested_arrival_rates = []
            nested_selectivity = []
            nested_cost = []
            if pattern.statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
                # If we have a selectivity matrix, than we need to create new one, that has the size of the "flat" root
                # operator and all its entries are computed according to the nested operators in each composite/kleene
                # structure.
                nested_selectivity = TreePlanBuilder._selectivity_matrix_for_nested_operators(pattern)
            for arg in pattern.positive_structure.get_args():
                # This loop creates (recursively) all the nested subtrees
                if not isinstance(arg, PrimitiveEventStructure):
                    # If we are here than this structure is composite or kleene.
                    # And first create new pattern that fits the nested operator's stats and structure.
                    pattern, nested_topologies, nested_arrival_rates, nested_cost, nested_args = \
                        self.handle_composite_or_unary_nested_pattern(arg, pattern, nested_topologies,
                                                                      nested_arrival_rates, nested_cost, nested_args)
                else:
                    # If we are here, than this structure is primitive
                    pattern, nested_topologies, nested_arrival_rates, nested_cost, nested_args = \
                        TreePlanBuilder.handle_primitive_event(pattern, nested_topologies, nested_arrival_rates,
                                                               nested_cost, nested_args)
            if pattern.statistics_type == StatisticsTypes.ARRIVAL_RATES:
                pattern.set_statistics(StatisticsTypes.ARRIVAL_RATES, nested_arrival_rates)
            elif pattern.statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
                pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (nested_selectivity, nested_arrival_rates))

        return pattern, nested_topologies, nested_args, nested_cost

    def handle_composite_or_unary_nested_pattern(self, arg, pattern, nested_topologies, nested_arrival_rates,
                                                 nested_cost, nested_args):
        nested_pattern = Pattern(arg, None, pattern.window)
        if pattern.statistics_type == StatisticsTypes.ARRIVAL_RATES:
            nested_pattern.set_statistics(StatisticsTypes.ARRIVAL_RATES, pattern.statistics)
        elif pattern.statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            (_, arrival_rates) = pattern.statistics
            # Take only the relevant part of the selectivity matrix:
            chopped_nested_selectivity = TreePlanBuilder._chop_matrix(pattern, arg)
            nested_pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES,
                                          (chopped_nested_selectivity, arrival_rates))
        nested_topology = self._create_tree_topology(nested_pattern)
        nested_topologies.append(nested_topology)  # Save nested topology to add it as a field to its root
        if pattern.statistics_type == StatisticsTypes.ARRIVAL_RATES or pattern.statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            # Get the cost of the nested structure to calculate the new arrival rate and save it for other
            # functions (all functions that uses cost of a tree and will need the cost of nested subtrees).
            cost = IntermediateResultsTreeCostModel().get_plan_cost(nested_pattern, nested_topology)
            if isinstance(arg, UnaryStructure):
                if isinstance(arg, KleeneClosureOperator):
                    nested_arrival_rates.append(pow(2, cost) / pattern.window.total_seconds())
                else:
                    raise Exception("unknown unary operator")
            else:
                nested_arrival_rates.append(cost / pattern.window.total_seconds())
            nested_cost.append(cost)
        else:
            nested_cost.append(None)
        nested_args.append(arg.get_args())
        return pattern, nested_topologies, nested_arrival_rates, nested_cost, nested_args

    @staticmethod
    def handle_primitive_event(pattern, nested_topologies, nested_arrival_rates, nested_cost, nested_args):
        nested_topologies.append(None)
        nested_args.append(None)
        nested_cost.append(None)
        if pattern.statistics_type == StatisticsTypes.ARRIVAL_RATES:
            nested_arrival_rates.append(pattern.statistics[0])
            pattern.statistics.pop(0)
        elif pattern.statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            (selectivity_matrix, arrival_rates) = pattern.statistics
            # Save the original arriving rate, because it is not nested, and pop it out, so we always look
            # on the arrival rate that fits the current arg in the loop.
            nested_arrival_rates.append(arrival_rates[0])
            arrival_rates.pop(0)
            pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES,
                                   (selectivity_matrix, arrival_rates))
        return pattern, nested_topologies, nested_arrival_rates, nested_cost, nested_args
