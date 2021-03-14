"""
This file contains the class responsible for evaluation manager initialization.
"""
from typing import List
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from parallel.ParallelExecutionParameters import *
from parallel.manager.SequentialEvaluationManager import SequentialEvaluationManager
from parallel.data_parallel.DataParallelEvaluationManager import DataParallelEvaluationManager


class EvaluationManagerFactory:
    """
    Creates an evaluation manager given its specification.
    """
    @staticmethod
    def create_evaluation_manager(patterns: Pattern or List[Pattern],
                                  eval_mechanism_params: EvaluationMechanismParameters,
                                  parallel_execution_params: ParallelExecutionParameters):
        if parallel_execution_params is None:
            parallel_execution_params = ParallelExecutionParameters()
        if parallel_execution_params.execution_mode == ParallelExecutionModes.SEQUENTIAL:
            return SequentialEvaluationManager(patterns, eval_mechanism_params)
        if parallel_execution_params.execution_mode == ParallelExecutionModes.DATA_PARALLELISM:
            return DataParallelEvaluationManager(patterns, eval_mechanism_params, parallel_execution_params)
        raise Exception("Unknown parallel execution mode: %s" % (parallel_execution_params.execution_mode,))
