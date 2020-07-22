from abc import ABC
from evaluation.LeftDeepTreeBuilders import GreedyLeftDeepTreeBuilder
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismFactory


class PlanGenerationAlgorithm(ABC):
    def generate_plan(self, SC_data):
        pass


class GreedyAlgorithm(PlanGenerationAlgorithm):
    """
    This plan generation algorithm is for Invariant based decisions
    """
    def __init__(self):
        self.builder = GreedyLeftDeepTreeBuilder()

    def generate_plan(self, SC_data):
        return self.builder.create_evaluation_order(SC_data.selectivity_matrix, SC_data.rates)

    def build_final_plan_for_evaluation(self, pattern: Pattern, new_plan):
        return self.builder.build_single_pattern_eval_mechanism(pattern, new_plan)


class PlanBuilder(PlanGenerationAlgorithm):
    """
    Will build a plan according to
    """
    def build_final_plan_for_evaluation(self, eval_mechanism_type, pattern):
        return EvaluationMechanismFactory.build_single_pattern_eval_mechanism(eval_mechanism_type, None, pattern)