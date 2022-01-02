from abc import ABC, abstractmethod


class Rule(ABC):
    @abstractmethod
    def validate_instructions(self) -> None:
        pass

    @abstractmethod
    def execute(self) -> None:
        pass
