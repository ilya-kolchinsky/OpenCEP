from datetime import timedelta, datetime
from statisticsCollector.ExponentialHistogramTypes import ExponentialHistogramEnum
import math


class Bucket:

    def __init__(self, size, timestamp: datetime):
        self.size = size
        self.timestamp = timestamp


class BasicCounting:

    def __init__(self, k: int, size_of_window: timedelta, exponential_histogram_type: ExponentialHistogramEnum = ExponentialHistogramEnum.BASIC_COUNTING_BINARY):
        self.eh_type = exponential_histogram_type
        self.slidingWindow = size_of_window
        self.currentTimestamp = 0
        self.counterTotal = 0
        self.counterLast = None
        self.k = k
        self.EH = []

    def update_sliding_window(self, size_of_window: int):
        self.slidingWindow = size_of_window

    def traverse_the_list(self):
        eh_size = len(self.EH)
        counter = 0
        size = 1
        i = 0
        while i < eh_size:
            if size != self.EH[i].size:
                size = self.EH[i].size
                counter = 1
            else:
                counter += 1

            if counter == math.ceil(self.k / 2) + 2:
                self.EH[i-1].size = self.EH[i].size * 2
                size = self.EH[i-1].size
                counter = 1
                if self.counterLast == self.EH[i]:
                    self.counterLast = self.EH[i - 1]
                del self.EH[i]
                eh_size -= 1
            else:
                i += 1
        return

    def expiry_time(self, time: datetime):
        res = time - self.slidingWindow
        return res

    def insert_event(self, element, timestamp: datetime):
        if self.counterLast is not None and self.counterLast.timestamp < self.expiry_time(timestamp):
            self.counterTotal = self.counterTotal - self.counterLast.size
            if len(self.EH) < 2:
                self.counterLast = None
                self.counterTotal = 0
            else:
                self.counterLast = self.EH[len(self.EH) - 2]
                del self.EH[-1]

        if element == 0:
            return

        if self.eh_type == ExponentialHistogramEnum.BASIC_COUNTING_BINARY:
            bucket = Bucket(1, timestamp)
            self.EH.insert(0, bucket)
            self.counterTotal += 1
            if self.counterLast is None:
                self.counterLast = bucket
            else:
                self.traverse_the_list()
        elif self.eh_type == ExponentialHistogramEnum.BASIC_COUNTING_POSITIVE_INTEGERS:
            while element > 0:
                bucket = Bucket(1, timestamp)
                self.EH.insert(0, bucket)
                self.counterTotal += 1
                element -= 1
                if self.counterLast is None:
                    self.counterLast = bucket
                else:
                    self.traverse_the_list()

    def get_total(self):
        return self.counterTotal