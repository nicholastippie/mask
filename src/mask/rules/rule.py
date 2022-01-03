from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class Rule(ABC):
    @abstractmethod
    def validate_instructions(self) -> None:
        pass

    @abstractmethod
    def execute(self) -> None:
        pass
