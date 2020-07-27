from abc import ABC
from datetime import timedelta, datetime
from statisticsCollector.ExponentialHistogramTypes import ExponentialHistogramEnum
import math


class Bucket:
    def __init__(self, size, timestamp: datetime):
        self.size = size
        self.timestamp = timestamp


class BasicCounting:

    def __init__(self, k: int, size_of_window: timedelta):
        self.__slidingWindow = size_of_window
        self.__currentTimestamp = 0
        self.__counterTotal = 0
        self.__counterLast = None
        self.__k = k
        self.EH = []

    def updateSlidingWindow(self, size_of_window: int):
        self.__slidingWindow = size_of_window

    def TraverseTheList(self):
        EHsize = len(self.EH)
        counter = 0
        size = 1
        i = 0
        while i < EHsize:
            if size != self.EH[i].size:
                size = self.EH[i].size
                counter = 1
            else:
                counter += 1

            if counter == math.ceil(self.__k / 2) + 2:
                self.EH[i-1].size = self.EH[i].size * 2
                size = self.EH[i-1].size
                counter = 1
                if self.__counterLast == self.EH[i]:
                    self.__counterLast = self.EH[i - 1]
                del self.EH[i]
                EHsize -= 1
            else:
                i += 1
        return

    def ExpiryTime(self, time: datetime):
        res = time - self.__slidingWindow
        return res


    def insert_event(self, element, timestamp: datetime):

        if self.__counterLast is not None and self.__counterLast.timestamp < self.ExpiryTime(timestamp):
            self.__counterTotal = self.__counterTotal - self.__counterLast.size
            if len(self.EH) < 2:
                self.__counterLast = None
                self.__counterTotal = 0
            else:
                self.__counterLast = self.EH[len(self.EH) - 2]
                del self.EH[-1]

        if element == 0:
            return

        if element == 1:
            bucket = Bucket(1, timestamp)
            self.EH.insert(0, bucket)
            self.__counterTotal += 1
            if self.__counterLast is None:
                self.__counterLast = bucket
            else:
                self.TraverseTheList()

    def get_total(self):
        return self.__counterTotal
