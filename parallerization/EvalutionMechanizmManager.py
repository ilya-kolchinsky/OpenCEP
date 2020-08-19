
from evaluation.EvaluationMechanismFactory import (
    EvaluationMechanismParameters,
    EvaluationMechanismTypes,
    EvaluationMechanismFactory,
)
from misc.IOUtils import Stream
from parallerization import EvaluationMechanismConfiguration
from base import Pattern

from misc.Utils import split_data


class EvaluationMechanismManager:

    def __init__(self, evaluation_mechanism_configuration: EvaluationMechanismConfiguration):
        self._evaluation_mechanism_configuration = evaluation_mechanism_configuration
        self._evaluation_mechanisms_list = []
        self._master = None

        #original event stream
        self._event_stream =  Stream()
        #array of Streams after we splitted the original event_stream according to user function
        self._event_stream_splitted =  []


    def build_evaluation_mechanisms(self):
        config = self._evaluation_mechanism_configuration

        execution_units = config._parallel_params._num_of_servers * config._parallel_params._num_of_processes

        if type(config._pattern) == Pattern:#if there is a single pattern
           source_eval_mechanism = EvaluationMechanismFactory.build_single_pattern_eval_mechanism(config._eval_mechanism_type,
                                                                                                  config._eval_mechanism_params,
                                                                                                  config._patterns[0])
           if execution_units > 1:
                self._evaluation_mechanisms, self._master = source_eval_mechanism.split(execution_units)
           else:
               self._evaluation_mechanisms.append(source_eval_mechanism)

        else:#in multi pattern mode
            raise NotImplementedError()#not implemented yet

        if config._splitted_data:
            self._event_stream_splitted = split_data(self._event_stream, config._data_split_func)
            num_of_data_parts = len(self._event_stream_splitted)
            #duplicate source_eval_mechanism num_of_data_parts times into self._evaluation_mechanisms_list


    def eval(self, event_stream, pattern_matches, is_async = False, file_path = None, time_limit = None):

         self.build_evaluation_mechanisms()

         #yet to be paralelized code
         for evaluation_mechanism in self._evaluation_mechanisms_list:
            evaluation_mechanism.eval()



