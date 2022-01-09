from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class Rule(ABC):
    group: int = 0

    @abstractmethod
    def validate_instructions(self) -> None:
        if self.group < 1:
            raise ValueError(f"Group must be 1 or greater for {self}")

    @abstractmethod
    def execute(self) -> None:
        pass
