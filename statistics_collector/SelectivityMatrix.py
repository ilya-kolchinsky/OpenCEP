from base.PatternStructure import PatternStructure, PrimitiveEventStructure
from base.Event import Event
from collections import Counter
from base.Pattern import Pattern
from datetime import timedelta
from statistics_collector.EventTime import EventTime
from condition.Condition import Condition


class SelectivityMatrix:
    def __init__(self, pattern: Pattern):
        # self.event_types = patterns.get_all_event_types()
        # self.arrival_rates = {}
        # self.events_arrival_time = []
        self.pattern = pattern
        # self.args = pattern.positive_structure.args
        self.args = list(pattern.get_all_event_types())
        self.args_num = len(self.args)
        self.selectivity_matrix = [[0.0 for _ in range(self.args_num)] for _ in range(self.args_num)]
        self.events_in_window = []
        self.window = pattern.window

    def update(self, event: Event):
        event_type = event.type
        # if event_type in self.event_types:
        #     self.arrival_rates[event_type] += 1
        #     self.events_arrival_time.append(EventTime(event.timestamp, event_type))
        # self.__remove_expired_events(event.timestamp)
        j = self.args.index(event_type)
        for i in range(self.args_num):
            new_sel = self.get_condition_selectivity(self.args[i], event_type,
                                                self.pattern.condition.get_condition_of({args[i].name, event_type.name}),
                                                self.events_in_window,
                                                self.pattern.positive_structure.get_top_operator() == SeqOperator)
            self.selectivity_matrix[i][j] = self.selectivity_matrix[j][i] = new_sel
        self.events_in_window.append(event)

    @staticmethod
    def __get_condition_selectivity(arg1: PrimitiveEventStructure, arg2: PrimitiveEventStructure,
                                    condition: Condition,
                                    window, is_sequence: bool):
        """
        Calculates the selectivity of a given condition between two event types by evaluating it on a given stream.
        """
        if condition is None:
            return 1.0

        count = 0
        match_count = 0

        if arg1 == arg2:
            for event in stream:
                if event.eventType == arg1.type:
                    count += 1
                    if condition.eval({arg1.name: event.event}):
                        match_count += 1
        else:
            events1 = []
            events2 = []
            for event in window:
                if event.eventType == arg1.type:
                    events1.append(event)
                elif event.eventType == arg2.type:
                    events2.append(event)
            for event1 in events1:
                for event2 in events2:
                    if (not is_sequence) or event1.date < event2.date:
                        count += 1
                        if condition.eval({arg1.name: event1.event, arg2.name: event2.event}):
                            match_count += 1
        return match_count / count

    def __remove_expired_events(self, timestamp: timedelta):
        # for i, event_time in enumerate(self.events_arrival_time):
        #     if timestamp - event_time.timestamp > self.window:
        #         self.arrival_rates[event_time.event_type] -= 1
        #     else:
        #         self.events_arrival_time = self.events_arrival_time[i:]
        #         break
        pass
