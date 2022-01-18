from mask.database.database_gateway import DatabaseGateway

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class Rule(ABC):
    group: int = 0
    database_gateway: DatabaseGateway = None

    @abstractmethod
    def validate_instructions(self) -> None:
        if not isinstance(self.group, int):
            raise ValueError(f"Group must be an integer for {self}")
        if self.group < 1:
            raise ValueError(f"Group must be 1 or greater for {self}")

    @abstractmethod
    def execute(self) -> None:
        pass
