from adaptive.statistics.StatisticsTypes import StatisticsTypes
from plan.negation.NaiveNegationAlgorithm import NaiveNegationAlgorithm
from plan.negation.NegationAlgorithm import *


class StatisticNegationAlgorithm(NaiveNegationAlgorithm):
    """
    This class represents the statistic negation algorithm.
    """
    def _add_negative_part(self, pattern: Pattern, statistics: Dict, positive_tree_plan: TreePlanBinaryNode,
                           all_negative_indices: List[int], unbounded_negative_indices: List[int]):
        if StatisticsTypes.ARRIVAL_RATES not in statistics:
            raise Exception("Cannot activate this algorithm when no arrival rates are given")
        negative_event_rates = {i: statistics[StatisticsTypes.ARRIVAL_RATES][i] for i in all_negative_indices}
        bounded_negative_indices = [i for i in all_negative_indices if i not in unbounded_negative_indices]

        bounded_negative_indices.sort(key=lambda x: negative_event_rates[x], reverse=True)
        unbounded_negative_indices.sort(key=lambda x: negative_event_rates[x], reverse=True)

        return super()._add_negative_part(pattern, statistics, positive_tree_plan,
                                          bounded_negative_indices + unbounded_negative_indices,
                                          unbounded_negative_indices)
