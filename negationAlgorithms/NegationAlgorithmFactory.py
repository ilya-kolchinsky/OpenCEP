from negationAlgorithms.LowestPositionNegationAlgorithm import LowestPositionNegationAlgorithm
from negationAlgorithms.NaiveNegationAlgorithm import NaiveNegationAlgorithm
from negationAlgorithms.NegationAlgorithmTypes import NegationAlgorithmTypes
from negationAlgorithms.StatisticNegationAlgorithm import StatisticNegationAlgorithm


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
            return NaiveNegationAlgorithm(negation_algorithm_type)
        elif negation_algorithm_type == NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM:
            return StatisticNegationAlgorithm(negation_algorithm_type)
        elif negation_algorithm_type == NegationAlgorithmTypes.LOWEST_POSITION_NEGATION_ALGORITHM:
            return LowestPositionNegationAlgorithm(negation_algorithm_type)
        raise Exception("Unknown negation algorithm type: %s" % (negation_algorithm_type,))
