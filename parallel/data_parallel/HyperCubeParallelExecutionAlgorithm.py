"""
 Data parallel HyperCube algorithms
"""
from abc import ABC
from parallel.data_parallel.DataParallelExecutionAlgorithm import DataParallelExecutionAlgorithm
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from math import floor
from base.PatternMatch import *
from functools import reduce
import numpy as np
from misc.Utils import is_int, is_float
from typing import Tuple


class HyperCubeParallelExecutionAlgorithm(DataParallelExecutionAlgorithm, ABC):
    """
    Implements the HyperCube algorithm.
    """

    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters, platform, attributes_dict: dict):
        dims = 0
        self.attributes_dict = dict()
        for k, v in attributes_dict.items():
            if isinstance(v, list):
                self.attributes_dict[k] = [(v, dims + i) for i, v in enumerate(v)]
                dims += len(v)
            elif isinstance(v, str):
                self.attributes_dict[k] = [(v, dims)]
                dims += 1
            else:
                raise Exception
        self.shares, self.cube_size = self._calc_cubic_shares(units_number, dims)
        self.cube = np.array(range(self.cube_size)).reshape(self.shares)
        super().__init__(self.cube_size, patterns, eval_mechanism_params, platform)

    def _classifier(self, event: Event):
        attributes = self.attributes_dict.get(event.type)
        if attributes:
            units = set()
            for attribute, index in attributes:
                # find for all attributes the union of the column/cube face
                indices = [slice(None)] * self.cube.ndim
                value = event.payload.get(attribute)
                if value is None:
                    raise Exception(f"attribute {attribute} is not existing in type {event.type}")
                elif not is_int(value) and not is_float(value):
                    raise Exception(f"Non numeric key {attribute} = {value}")
                col = int(value) % self.shares[index]
                indices[index] = slice(col, col + 1)
                units.update(self.cube[tuple(indices)].reshape(-1))
            return units
        return range(self.cube_size)

    @staticmethod
    def _calc_cubic_shares(units_number, dims) -> Tuple[Tuple[int], int]:
        # find the most equal share, and try to improve it with extra units
        # for example: _calc_cubic_shares(10, 2) = [3,3] not [5,2]
        # max(shares)-min(shares) <= 1
        equal_share = floor(units_number ** (1 / dims))
        shares = [equal_share] * dims

        def used_units() -> int:
            return reduce(lambda a, b: a * b, shares)

        change = True
        while change:
            change = False
            for s in range(len(shares)):
                if used_units() / shares[s] * (shares[s] + 1) <= units_number:
                    shares[s] += 1
                    change = True
                else:
                    continue
        return shares, used_units()


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
