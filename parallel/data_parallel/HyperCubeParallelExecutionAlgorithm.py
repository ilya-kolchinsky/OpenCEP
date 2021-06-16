"""
 Data parallel HyperCube algorithms
"""
from abc import ABC
from parallel.data_parallel.DataParallelExecutionAlgorithm import DataParallelExecutionAlgorithm
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import \
    EvaluationMechanismParameters
from math import floor
from base.PatternMatch import *
from functools import reduce
import numpy as np


class HyperCubeParallelExecutionAlgorithm(DataParallelExecutionAlgorithm, ABC):
    """
    Implements the HyperCube algorithm.
    """

    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters, platform, attributes_dict: dict):
        if isinstance(patterns, list):
            if len(patterns) > 1:
                raise Exception  # TODO: check if possible multiple patterns
            pattern = patterns[0]
        else:
            pattern = patterns
        dims = 0
        self.attributes_dict = dict()
        for k, v in attributes_dict.items():
            if isinstance(v, list):
                if len(v)>1:
                    raise Exception # TODO: add
                self.attributes_dict[k] = [(v, dims + i) for i, v in enumerate(v)]
                dims += len(v)
            elif isinstance(v, str):
                self.attributes_dict[k] = [(v, dims)]
                dims += 1
            else:
                raise Exception
        self.shares = self._calc_shares(units_number, dims)
        self.cube_size = reduce(lambda a, b: a * b, self.shares)
        super().__init__(self.cube_size, pattern, eval_mechanism_params, platform)
        self.cube = np.array(range(self.cube_size)).reshape(self.shares)

    def _classifier(self, event: Event):
        indices = [slice(None)] * self.cube.ndim
        attributes = self.attributes_dict.get(event.type)
        if attributes:
            attribute, index = attributes[0]
            value = event.payload.get(attribute)
            if value is None:
                raise Exception
            col = int(value) % self.shares[index]
            indices[index] = slice(col, col+1)
        return self.cube[tuple(indices)].reshape(-1).tolist()

    def _calc_shares(self, units_number, dims):
        equal_share = floor(units_number ** (1 / dims))
        shares = [equal_share for _ in range(dims)]
        change = True
        while change:
            change = False
            for s in range(len(shares)):
                if reduce(lambda a, b: a * b, shares) / shares[s] * (shares[s] + 1) <= units_number:
                    shares[s] += 1
                    change = True
                else:
                    continue
        return shares

    # @staticmethod
    # def _list_to_ndarray(l, dims):
    #     def list_to_matrix(l, n):
    #         if len(l) % n:
    #             raise Exception("not fit")
    #         m = len(l) // n
    #         return [[l[i + n * j] for i in range(n)] for j in range(m)]
    #
    #     for d in dims[-1:0:-1]:
    #         l = list_to_matrix(l, d)
    #     return l
