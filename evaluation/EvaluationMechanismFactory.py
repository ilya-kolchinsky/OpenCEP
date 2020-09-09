from typing import List
from enum import Enum

from base.Pattern import Pattern
from plan.BushyTreeBuilders import DynamicProgrammingBushyTreeBuilder, ZStreamTreeBuilder, ZStreamOrdTreeBuilder
from plan.IterativeImprovement import IterativeImprovementType
from plan.LeftDeepTreeBuilders import IterativeImprovementInitType, TrivialLeftDeepTreeBuilder, \
    AscendingFrequencyTreeBuilder, GreedyLeftDeepTreeBuilder, IterativeImprovementLeftDeepTreeBuilder, \
    DynamicProgrammingLeftDeepTreeBuilder
from tree.PartialMatchStorage import TreeStorageParameters


class EvaluationMechanismTypes(Enum):
    """
    The various algorithms for constructing an efficient evaluation tree.
    """
    TRIVIAL_LEFT_DEEP_TREE = 0,
    SORT_BY_FREQUENCY_LEFT_DEEP_TREE = 1,
    GREEDY_LEFT_DEEP_TREE = 2,
    LOCAL_SEARCH_LEFT_DEEP_TREE = 3,
    DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE = 4,
    DYNAMIC_PROGRAMMING_BUSHY_TREE = 5,
    ZSTREAM_BUSHY_TREE = 6,
    ORDERED_ZSTREAM_BUSHY_TREE = 7


class EvaluationMechanismParameters:
    """
    Parameters for the evaluation mechanism builder.
    """
    def __init__(self, eval_mechanism_type: EvaluationMechanismTypes):
        self.type = eval_mechanism_type


class TreeBasedEvaluationMechanismParameters(EvaluationMechanismParameters):
    """
    shared Parameters for the Tree Based evaluation mechanism builders.
    """
    def __init__(self, eval_mechanism_type: EvaluationMechanismTypes = EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE,
                 storage_params: TreeStorageParameters = TreeStorageParameters()):
        super().__init__(eval_mechanism_type)
        self.storage_params = storage_params


class IterativeImprovementEvaluationMechanismParameters(TreeBasedEvaluationMechanismParameters):
    """
    Parameters for evaluation mechanism builders based on local search include the number of search steps, the
    choice of the neighborhood (step) function, and the way to generate the initial state.
    """
    def __init__(self, step_limit: int,
                 ii_type: IterativeImprovementType = IterativeImprovementType.SWAP_BASED,
                 init_type: IterativeImprovementInitType = IterativeImprovementInitType.RANDOM,
                 storage_params: TreeStorageParameters = TreeStorageParameters()):
        super().__init__(EvaluationMechanismTypes.LOCAL_SEARCH_LEFT_DEEP_TREE, storage_params)
        self.ii_type = ii_type
        self.init_type = init_type
        self.step_limit = step_limit


class EvaluationMechanismFactory:
    """
    Creates an evaluation mechanism given its specification.
    """

    @staticmethod
    def build_single_pattern_eval_mechanism(eval_mechanism_type: EvaluationMechanismTypes,
                                            eval_mechanism_params: EvaluationMechanismParameters,
                                            pattern: Pattern):
        storage_params = eval_mechanism_params.storage_params \
                         if isinstance(eval_mechanism_params, TreeBasedEvaluationMechanismParameters) \
                         else TreeStorageParameters()
        eval_mechanism_builder = EvaluationMechanismFactory.__create_eval_mechanism_builder(eval_mechanism_type,
                                                                                            eval_mechanism_params)
        return eval_mechanism_builder.build_single_pattern_eval_mechanism(pattern, storage_params)

    @staticmethod
    def build_multi_pattern_eval_mechanism(eval_mechanism_type: EvaluationMechanismTypes,
                                           eval_mechanism_params: EvaluationMechanismParameters,
                                           patterns: List[Pattern]):
        storage_params = eval_mechanism_params.storage_params \
                         if isinstance(eval_mechanism_params, TreeBasedEvaluationMechanismParameters) \
                         else TreeStorageParameters()
        eval_mechanism_builder = EvaluationMechanismFactory.__create_eval_mechanism_builder(eval_mechanism_type,
                                                                                            eval_mechanism_params)
        return eval_mechanism_builder.build_multi_pattern_eval_mechanism(patterns, storage_params)

    @staticmethod
    def __create_eval_mechanism_builder(eval_mechanism_type: EvaluationMechanismTypes,
                                        eval_mechanism_params: EvaluationMechanismParameters):
        eval_mechanism_params = EvaluationMechanismFactory.__create_eval_mechanism_parameters(eval_mechanism_type,
                                                                                              eval_mechanism_params)
        if eval_mechanism_params.type == EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE:
            return TrivialLeftDeepTreeBuilder()
        if eval_mechanism_params.type == EvaluationMechanismTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE:
            return AscendingFrequencyTreeBuilder()
        if eval_mechanism_params.type == EvaluationMechanismTypes.GREEDY_LEFT_DEEP_TREE:
            return GreedyLeftDeepTreeBuilder()
        if eval_mechanism_params.type == EvaluationMechanismTypes.LOCAL_SEARCH_LEFT_DEEP_TREE:
            return IterativeImprovementLeftDeepTreeBuilder(eval_mechanism_params.step_limit,
                                                           eval_mechanism_params.ii_type,
                                                           eval_mechanism_params.init_type)
        if eval_mechanism_params.type == EvaluationMechanismTypes.DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE:
            return DynamicProgrammingLeftDeepTreeBuilder()
        if eval_mechanism_params.type == EvaluationMechanismTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE:
            return DynamicProgrammingBushyTreeBuilder()
        if eval_mechanism_params.type == EvaluationMechanismTypes.ZSTREAM_BUSHY_TREE:
            return ZStreamTreeBuilder()
        if eval_mechanism_params.type == EvaluationMechanismTypes.ORDERED_ZSTREAM_BUSHY_TREE:
            return ZStreamOrdTreeBuilder()
        return None

    @staticmethod
    def __create_eval_mechanism_parameters(eval_mechanism_type: EvaluationMechanismTypes,
                                           eval_mechanism_params: EvaluationMechanismParameters):
        if eval_mechanism_params is not None:
            return eval_mechanism_params
        return EvaluationMechanismParameters(eval_mechanism_type)
