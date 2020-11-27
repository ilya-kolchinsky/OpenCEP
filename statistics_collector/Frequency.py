from collections import Counter, deque

class Frequency():
    def __init__(self, event_types):
        self.statistics = Counter(event_types)

    def update(self,event):
        pass