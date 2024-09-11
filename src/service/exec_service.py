from abc import ABC, abstractmethod

class IExecutableService(ABC):
    """
    Callable service to run inside workflow
    """
    @abstractmethod
    def exec(self):
        pass