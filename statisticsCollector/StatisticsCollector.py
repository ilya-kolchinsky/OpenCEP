from base.Pattern import Pattern
from statisticsCollector.StatisticsTypes import StatisticsTypes
from statisticsCollector.ExponentialHistogram import BasicCounting
from statisticsCollector.ExponentialHistogramTypes import ExponentialHistogramEnum
from base.Event import Event
from copy import deepcopy


class Stat:
    def __init__(self, pattern: Pattern, statistics_type):
        self.statistics_type = statistics_type
        if self.statistics_type == StatisticsTypes.NO_STATISTICS:
            return None

        args = pattern.structure.args
        args_num = len(args)
        self.arrival_rates = {}
        for arg in args:
            self.arrival_rates[arg] = 0

        if self.statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            self.selectivity_matrix = [[0.0 for _ in range(args_num)] for _ in range(args_num)]
            for i in range(args_num):
                self.selectivity_matrix[i][i] = 1.0
        elif self.statistics_type == StatisticsTypes.FREQUENCY_DICT:
            raise NotImplementedError("FREQUENCY_DICT statistics not supported")


class StatisticsCollector:
    def __init__(self, pattern: Pattern, statistics_type: StatisticsTypes, window_coefficient: int, k: int):
        self.statistics_type = statistics_type
        self.stat = Stat(pattern, self.statistics_type)
        self.eh_type = pattern.exponential_histogram_type
        self.eh_rates = {}
        self.pattern = pattern
        self.k = k
        self.window_coefficient = window_coefficient

        if self.statistics_type == StatisticsTypes.NO_STATISTICS:
            return
        # get all types of events and create EH for every event in the pattern
        args = pattern.structure.args
        args_num = len(args)
        for arg in args:
            self.eh_rates[arg] = BasicCounting(k, window_coefficient * pattern.window)

        if self.statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            list_of_eh = []
            self.eh_selectivitys = [[list_of_eh for _ in range(args_num)] for _ in range(args_num)]

    def insert_event(self, event: Event):
        if self.statistics_type == StatisticsTypes.NO_STATISTICS:
            return

        if self.eh_type == ExponentialHistogramEnum.BASIC_COUNTING_BINARY:
            for key in self.eh_rates:
                if key.event_type == event.event_type:
                    self.eh_rates[key].insert_event(1, event.timestamp)
                self.eh_rates[key].insert_event(0, event.timestamp)
                self.stat.arrival_rates[key] = self.eh_rates[key].get_total()

    def update_selectivitys_matrix(self, event:Event, matches: dict):
        if self.statistics_type == StatisticsTypes.NO_STATISTICS:
            return

        index_dict = {}
        QTtem_event = None
        i = 0
        args = self.pattern.structure.args
        for arg in args:
            index_dict[arg.event_type] = i
            if arg.event_type == event.event_type:
                QTtem_event = arg
            i += 1

        for arg in args:
            match_count = 0
            count = self.eh_rates[QTtem_event].get_total() * self.eh_rates[arg].get_total()
            index1 = index_dict[event.event_type]
            index2 = index_dict[arg.event_type]
            if index1 == index2:
                continue
            for eh in self.eh_selectivitys[index1][index2]:
                eh.insert_event(0, event.timestamp)
            for eh in self.eh_selectivitys[index2][index1]:
                eh.insert_event(0, event.timestamp)
            if len(matches[arg.event_type]) > 0:
                eh = BasicCounting(self.k, self.window_coefficient * self.pattern.window)
                for timestamp in matches[arg.event_type]:
                    eh.insert_event(1, timestamp)
                self.eh_selectivitys[index1][index2].insert(0, eh)
                self.eh_selectivitys[index2][index1].insert(0, eh)
            for eh in self.eh_selectivitys[index1][index2]:
                match_count += eh.get_total()
            if count == 0:
                self.stat.selectivity_matrix[index1][index2] = self.stat.selectivity_matrix[index2][index1] = 0
            else:
                self.stat.selectivity_matrix[index1][index2] = self.stat.selectivity_matrix[index2][index1] = match_count / count

    def get_stat(self):
        return deepcopy(self.stat)
