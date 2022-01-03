from abc import ABC, abstractmethod
from dataclasses import dataclass

from mask.database_access.database_gateway import DatabaseGateway


@dataclass
class Rule(ABC):
    @abstractmethod
    def validate_instructions(self) -> None:
        pass

    @abstractmethod
    def execute(self) -> None:
        pass


@dataclass
class DataRule(Rule):
    database: str = ""
    schema: str = ""
    table: str = ""
    database_gateway: DatabaseGateway = None

    def validate_instructions(self) -> None:
        if self.database == "":
            raise ValueError(f"'database' property not set for {self}")
        if self.schema == "":
            raise ValueError(f"'schema' property not set for {self}")
        if self.table == "":
            raise ValueError(f"'table' property not set for {self}")

    def execute(self) -> None:
        pass


@dataclass
class DatabaseObjectRule(Rule):
    database: str = ""
    schema: str = ""
    table: str = ""
    database_gateway: DatabaseGateway = None

    def validate_instructions(self) -> None:
        if self.database == "*":
            raise ValueError(f"Wildcard character is not allowed for 'database' property "
                             f"for {self}")
        if self.table == "*":
            raise ValueError(f"Wildcard character is not allowed for 'table' property "
                             f"for {self}")

    def execute(self) -> None:
        pass
