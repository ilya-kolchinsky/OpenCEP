from abc import ABC, abstractmethod

from base.Event import Event


class Statistics(ABC):

    @abstractmethod
    def update(self, event: Event):
        pass
