from abc import ABC


class ParallelExecutionFramework(ABC):

    def __init__(self, evaluation_mechanism):
        self._evaluation_mechanism = evaluation_mechanism

    def eval(self, event_stream, pattern_matches):
        raise NotImplementedError()

    def get_data(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    # every evaluation mechanism which is not master should return None
    # master will return something only when all other finished
    # the function should wait and not return till final results are available in pattern_matches var.
    def get_final_results(self, pattern_matches):
        raise NotImplementedError()

    #def restart_state_for_next_run(self):
    #    raise NotImplementedError()

    def wait_till_finish(self):
        raise NotImplementedError()