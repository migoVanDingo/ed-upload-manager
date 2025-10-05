from abc import ABC, abstractmethod


class AbstractHandler(ABC):
    @abstractmethod
    def do_process(self):
        pass
