from misc import DefaultConfig


class RuleTransformationParameters:
    """
    Parameters to set the order of rules to be applied.
    All rules in directive will be enabled and will be applied in the order in which they appear in directive
    """
    def __init__(self,
                 rules_directive: list = DefaultConfig.DEFAULT_RULES_DIRECTIVE):
        self.rules_directive = rules_directive
