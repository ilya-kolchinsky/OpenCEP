from misc import DefaultConfig
from plan.multi.MultiPatternEvaluationApproaches import MultiPatternEvaluationApproaches


class MultiPatternEvaluationParameters:
    """
    Parameters for multi-pattern evaluation mode.
    """
    def __init__(self,
                 multi_pattern_eval_approach: MultiPatternEvaluationApproaches = DefaultConfig.MULTI_PATTERN_APPROACH):
        self.approach = multi_pattern_eval_approach
