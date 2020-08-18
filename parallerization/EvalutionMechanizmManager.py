from numpy import size

from evaluation import EvaluationMechanismFactory
from misc.IOUtils import Stream
from parallerization import EvaluationMechanismConfiguration


class EvaluationMechanismManager:

    def __init__(self, evalution_mechanizm_configuration: EvaluationMechanismConfiguration):

        self._evalution_mechanizm_configuration = evalution_mechanizm_configuration

        self.__evaluation_machamizms =  []
        self.__master =  None

        self.__event_stream =  Stream()

        self.__event_stream_splitted =  []


    def bulild_evaluation_machamizms(self):
        confg = self.evalution_mechanizm_configuration

        execution_units = num_of_servers * num_of_proc

        if (confg.splitted_data == True):
            self.event_stream_splitted = self._evalution_mechanizm_configuration.split_data(self.__event_stream)

        if (size(self.evalution_mechanizm_configuration._pattern) == 1):

           source_eval_mechanism = EvaluationMechanismFactory.\
               build_single_pattern_eval_mechanism( confg._eval_mechanism_type, confg._eval_mechanism_params, confg._pattern)

           if execution_units > 1:
                self.__evaluation_machamizms, self.__master = source_eval_mechanism.split(execution_units)
           else:
               self.__evaluation_machamizms.append(eval_mechanism)


        else:
            pass




    def eval(self, event_stream, __pattern_matches, is_async, file_path, time_limit):

         self.bulild_evaluation_machamizms(event_stream)

         for evalution_mechanizm: self.__evaluation_machamizms:
            evalution_mechanizm.eval()



