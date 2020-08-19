

class InputParallelParameters:
    #class that contains all of the parameters related to paralelization
    #must be created by the user if he wants to paralelize but has default parameters for everything

    def __init__(self, is_data_splitted: bool = False, func = None, distributed: bool = False,
                 num_of_servers: int = 1, num_of_processes: int = 1):

        self.is_data_splitted = is_data_splitted
        self.data_split_function = func
        self.is_distributed = distributed
        self.num_of_servers = num_of_servers
        self.num_of_processes = num_of_processes
