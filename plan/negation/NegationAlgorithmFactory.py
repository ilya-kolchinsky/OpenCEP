from plan.negation.LowestPositionNegationAlgorithm import LowestPositionNegationAlgorithm
from plan.negation.NaiveNegationAlgorithm import NaiveNegationAlgorithm
from plan.negation.NegationAlgorithmTypes import NegationAlgorithmTypes
from plan.negation.StatisticNegationAlgorithm import StatisticNegationAlgorithm


class NegationAlgorithmFactory:
    """
    A factory for instantiating the tree evaluation mechanism negation algorithm object.
    """
    @staticmethod
    def create_negation_algorithm(negation_algorithm_type: NegationAlgorithmTypes):
        """
        Returns a cost model of the specified type.
        """
        if negation_algorithm_type == NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM:
            return NaiveNegationAlgorithm()
        elif negation_algorithm_type == NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM:
            return StatisticNegationAlgorithm()
        elif negation_algorithm_type == NegationAlgorithmTypes.LOWEST_POSITION_NEGATION_ALGORITHM:
            return LowestPositionNegationAlgorithm()
        raise Exception("Unknown negation algorithm type: %s" % (negation_algorithm_type,))
