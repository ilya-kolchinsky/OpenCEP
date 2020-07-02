from typing import List
from enum import Enum

from base.Pattern import Pattern
from evaluation.BushyTreeBuilders import DynamicProgrammingBushyTreeBuilder, ZStreamTreeBuilder, ZStreamOrdTreeBuilder
from evaluation.IterativeImprovement import IterativeImprovementType
from evaluation.LeftDeepTreeBuilders import IterativeImprovementInitType, TrivialLeftDeepTreeBuilder, \
    AscendingFrequencyTreeBuilder, GreedyLeftDeepTreeBuilder, IterativeImprovementLeftDeepTreeBuilder, \
    DynamicProgrammingLeftDeepTreeBuilder
from evaluation.Storage import TreeStorageParameters


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


class IterativeImprovementEvaluationMechanismParameters(EvaluationMechanismParameters):
    """
    Parameters for evaluation mechanism builders based on local search include the number of search steps, the
    choice of the neighborhood (step) function, and the way to generate the initial state.
    """
    def __init__(self, step_limit: int,
                 ii_type: IterativeImprovementType = IterativeImprovementType.SWAP_BASED,
                 init_type: IterativeImprovementInitType = IterativeImprovementInitType.RANDOM):
        super().__init__(EvaluationMechanismTypes.LOCAL_SEARCH_LEFT_DEEP_TREE)
        self.ii_type = ii_type
        self.init_type = init_type
        self.step_limit = step_limit


class EvaluationMechanismFactory:
    """
    Creates an evaluation mechanism given its specification.
    """

    @staticmethod
    def build_single_pattern_eval_mechanism(
        eval_mechanism_type: EvaluationMechanismTypes,
        eval_mechanism_params: EvaluationMechanismParameters,
        pattern: Pattern,
        storage_params: TreeStorageParameters,
    ):
        return EvaluationMechanismFactory.__create_eval_mechanism_builder(
            eval_mechanism_type, eval_mechanism_params
        ).build_single_pattern_eval_mechanism(pattern, storage_params)

    @staticmethod
    def build_multi_pattern_eval_mechanism(
        eval_mechanism_type: EvaluationMechanismTypes,
        eval_mechanism_params: EvaluationMechanismParameters,
        patterns: List[Pattern],
    ):
        return EvaluationMechanismFactory.__create_eval_mechanism_builder(
            eval_mechanism_type, eval_mechanism_params
        ).build_multi_pattern_eval_mechanism(patterns)

    @staticmethod
    def __create_eval_mechanism_builder(
        eval_mechanism_type: EvaluationMechanismTypes, eval_mechanism_params: EvaluationMechanismParameters
    ):
        eval_mechanism_params = EvaluationMechanismFactory.__create_eval_mechanism_parameters(
            eval_mechanism_type, eval_mechanism_params
        )
        if eval_mechanism_params.type == EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE:
            return TrivialLeftDeepTreeBuilder()
        if eval_mechanism_params.type == EvaluationMechanismTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE:
            return AscendingFrequencyTreeBuilder()
        if eval_mechanism_params.type == EvaluationMechanismTypes.GREEDY_LEFT_DEEP_TREE:
            return GreedyLeftDeepTreeBuilder()
        if eval_mechanism_params.type == EvaluationMechanismTypes.LOCAL_SEARCH_LEFT_DEEP_TREE:
            return IterativeImprovementLeftDeepTreeBuilder(
                eval_mechanism_params.step_limit, eval_mechanism_params.ii_type, eval_mechanism_params.init_type
            )
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
    def __create_eval_mechanism_parameters(
        eval_mechanism_type: EvaluationMechanismTypes, eval_mechanism_params: EvaluationMechanismParameters
    ):
        if eval_mechanism_params is not None:
            return eval_mechanism_params
        return EvaluationMechanismParameters(eval_mechanism_type)
