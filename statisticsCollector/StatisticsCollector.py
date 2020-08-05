from base.Pattern import Pattern
from statisticsCollector.StatisticsTypes import StatisticsTypes
from statisticsCollector.ExponentialHistogram import BasicCounting
from statisticsCollector.ExponentialHistogramTypes import ExponentialHistogramEnum
from base.Event import Event
from copy import deepcopy

from base.PatternStructure import AndOperator, SeqOperator, QItem
from base.Formula import GreaterThanFormula, SmallerThanFormula, SmallerThanEqFormula, GreaterThanEqFormula, MulTerm, EqFormula, IdentifierTerm, AtomicTerm, AndFormula, TrueFormula
from datetime import timedelta
from misc.IOUtils import file_input, file_output
from misc.Stocks import MetastockDataFormatter

class Stat:
    """
    Stat contains arrival rates and selectivity matrix according to statistics type
    Stat is created by the StatisticsCollector class
    """
    def __init__(self, pattern: Pattern, statistics_type: StatisticsTypes):
        self.statistics_type = statistics_type

        if self.statistics_type == StatisticsTypes.NO_STATISTICS:
            return None

        # initiate dict in size of args in pattern, where the keys are Qitem
        # contains the arrival rates of the args in pattern
        args = pattern.structure.args
        args_num = len(args)
        self.arrival_rates = {}
        for arg in args:
            self.arrival_rates[arg.event_type] = 0

        # if statistics type includes selectivity matrix initiate matrix in size of args in pattern
        # contains the selectivity value for every couple of args in pattern
        # ordered according to the appearance of args in pattern
        if self.statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            self.selectivity_matrix = [[0.0 for _ in range(args_num)] for _ in range(args_num)]
            for i in range(args_num):
                self.selectivity_matrix[i][i] = 1.0


class StatisticsCollector:
    """
    StatisticsCollector the stat witch provides statistics according to statistics type
    initiate Exponential Histogram for rates (according to ExponentialHistogramEnum) and selectivity matrix
    - window coefficient: in case we want to collect statistics about the events in longer period the the sliding window
            event will expire from the exponential histogram after window_coefficient * sliding_window
    - k: param for the exponential histogram, detailed in "Algorithm for BasicCounting"
    """
    def __init__(self, pattern: Pattern, statistics_type: StatisticsTypes, window_coefficient: int, k: int,
                 exponential_histogram_type: ExponentialHistogramEnum = ExponentialHistogramEnum.BASIC_COUNTING_BINARY):
        self.statistics_type = statistics_type
        if self.statistics_type == StatisticsTypes.FREQUENCY_DICT:
            raise NotImplementedError("FREQUENCY_DICT statistics not supported")
        self.stat = Stat(pattern, self.statistics_type)
        self.exponential_histogram_type = exponential_histogram_type
        self.eh_rates = {}
        self.pattern = pattern
        self.k = k
        self.window_coefficient = window_coefficient

        if self.statistics_type == StatisticsTypes.NO_STATISTICS:
            return None
        # initiate list of exponential histograms, one for every event type, witch will calculate
        # the rate of every event
        # initiate dict in size of args in pattern, where the keys are event types, for getting
        # the index of event in the selectivity matrix
        args = pattern.structure.args
        args_num = len(args)
        self.event_index = {}

        for arg in args:
            self.eh_rates[arg.event_type] = BasicCounting(k, window_coefficient * pattern.window, self.exponential_histogram_type)

        # initiate selectivity matrix
        # initiate dict in size of args in pattern, where the keys are event types, for getting
        # the index of event in the selectivity matrix
        if self.statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            i = 0
            for arg in args:
                self.event_index[arg.event_type] = i
                i += 1
            self.eh_selectivity = [[BasicCounting(k, window_coefficient * pattern.window) for _ in range(args_num)] for _ in range(args_num)]

    def insert_event(self, event: Event):
        """
        in arrival of new event, updates the relevant histogram
        updates all other histograms and the Stat arrival rates
        """
        if self.statistics_type == StatisticsTypes.NO_STATISTICS:
            return
        if self.exponential_histogram_type == ExponentialHistogramEnum.BASIC_COUNTING_BINARY:
            for key in self.eh_rates:
                if key == event.event_type:
                    self.eh_rates[key].insert_event(1, event.timestamp)
                else:
                    self.eh_rates[key].insert_event(0, event.timestamp)
                self.stat.arrival_rates[key] = self.eh_rates[key].get_total()

    def update_selectivity_matrix(self, events_for_new_match):
        """
        updates the selectivity matrix when partial match is found in the tree
        """
        if self.statistics_type == StatisticsTypes.NO_STATISTICS or self.statistics_type == StatisticsTypes.ARRIVAL_RATES:
            return
        # find the time of the last event in "events_for_new_match"
        time = None
        for event in events_for_new_match:
            if time is None:
                time = event.timestamp
            elif event.timestamp > time:
                time = event.timestamp
        # for every match update the exponential histogram matrix
        list_of_index = []
        for event in events_for_new_match:
            list_of_index.insert(0, self.event_index[event.event_type])
        i = j = 0
        for i in range(len(self.pattern.structure.args)):
            for j in range(i+1):
                if i == j:
                    continue
                if i in list_of_index and j in list_of_index:
                    self.eh_selectivity[i][j].insert_event(1, time)
                    self.eh_selectivity[j][i].insert_event(1, time)
                else:
                    self.eh_selectivity[i][j].insert_event(0, time)
                    self.eh_selectivity[j][i].insert_event(0, time)
        # for every match update the the selectivity matrix
        event1 = event2 = None
        for i in range(len(self.pattern.structure.args)):
            for j in range(i+1):
                if i == j:
                    continue
                match_count = self.eh_selectivity[i][j].get_total()
                for arg in self.event_index.keys():
                    if i == self.event_index[arg]:
                        event1 = arg
                    if j == self.event_index[arg]:
                        event2 = arg

                if event1 == event2:
                    continue
                count = self.eh_rates[event1].get_total() * self.eh_rates[event2].get_total()
                if count == 0:
                    continue
                self.stat.selectivity_matrix[i][j] = self.stat.selectivity_matrix[j][i] = match_count / count

    def get_stat(self):
        """
        return copy of Stat
        """
        copy_stat = deepcopy(self.stat)
        arrival_rates_list = []
        for arg in self.pattern.structure.args:
            arrival_rates_list.append(copy_stat.arrival_rates[arg.event_type])
        copy_stat.arrival_rates = arrival_rates_list
        return copy_stat

def check_results(test_num: int, result):
    if test_num == 1:
        if result == [1, 0, 0, 0]:
            return
    elif test_num == 2:
        if result == [2, 0, 0, 0]:
            return
    elif test_num == 3:
        if result == [2, 1, 0, 0]:
            return
    elif test_num == 4:
        if result == [1, 1, 0, 0]:
            return
    elif test_num == 5:
        if result == [1, 1, 0, 0]:
            return
    elif test_num == 6:
        if result == [10, 0, 0, 0]:
            return
    elif test_num == 7:
        if result == [[1.0, 10/20, 0.0, 0.0], [10/20, 1.0, 0.0, 0.0], [0.0, 0.0, 1.0, 0.0], [0.0, 0.0, 0.0, 1.0]]:
            return
    elif test_num == 8:
        if result == [10, 2, 1, 1]:
            return
    elif test_num == 9:
        if result == [[1.0, 11/20, 1/10, 1/10], [11/20, 1.0, 0.5, 0.5], [1/10, 0.5, 1.0, 1/1], [1/10, 0.5, 1/1, 1.0]]:
            print("Statistics Collector Test succeeded")
            return
    print("Failed test number: ", test_num)


if __name__ == '__main__':
    statisticsCollectorTest = file_input("../test/EventFiles/StatisticsCollectorTest.txt", MetastockDataFormatter())
    pattern = Pattern(SeqOperator([QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("ORLY", "c"), QItem("CBRL", "d")]),
        AndFormula(
            AndFormula(
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                                   IdentifierTerm("b", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                                   IdentifierTerm("c", lambda x: x["Peak Price"]))
            ),
            SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                               IdentifierTerm("d", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    statistics_collector = StatisticsCollector(pattern, StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, 1, 3)

    # check arrival rate -
    statistics_collector.insert_event(statisticsCollectorTest.get_item())
    check_results(1, statistics_collector.get_stat().arrival_rates)

    statistics_collector.insert_event(statisticsCollectorTest.get_item())
    check_results(2, statistics_collector.get_stat().arrival_rates)

    statistics_collector.insert_event(statisticsCollectorTest.get_item())
    check_results(3, statistics_collector.get_stat().arrival_rates)

    statistics_collector.insert_event(statisticsCollectorTest.get_item())
    check_results(4, statistics_collector.get_stat().arrival_rates)

    statistics_collector.insert_event(statisticsCollectorTest.get_item())
    check_results(5, statistics_collector.get_stat().arrival_rates)

    event1 = statisticsCollectorTest.get_item()
    for i in range(10+1):
        statistics_collector.insert_event(event1)
    check_results(6, statistics_collector.get_stat().arrival_rates)

    event2 = statisticsCollectorTest.get_item()
    statistics_collector.insert_event(event2)
    statistics_collector.insert_event(event2)
    for i in range(10):
        statistics_collector.update_selectivity_matrix([event1, event2])
    check_results(7, statistics_collector.get_stat().selectivity_matrix)

    event3 = statisticsCollectorTest.get_item()
    event4 = statisticsCollectorTest.get_item()
    statistics_collector.insert_event(event3)
    statistics_collector.insert_event(event4)
    check_results(8, statistics_collector.get_stat().arrival_rates)

    statistics_collector.update_selectivity_matrix([event1, event2, event3, event4])
    check_results(9, statistics_collector.get_stat().selectivity_matrix)












