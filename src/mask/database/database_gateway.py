from abc import ABC, abstractmethod


class DatabaseGateway(ABC):
    """ Interface for database access """

    @abstractmethod
    def generate_where_clause_from_record(self, record: dict, primary_key: list) -> str:
        """
        Create a where clause from a record such that the where clause can distinctly
        identify the record in the database.

        :param record: Dictionary containing column names as keys and column values as values
        :param primary_key: List of primary key column names
        :return: String where clause which can identify the record in the database
        """
        ...

    @abstractmethod
    def generate_update_set_clause_for_column(self, column: str, replacement_value: vars) -> str: ...

    @abstractmethod
    def append_where_column_is_not_null(self, column: str, where_clause: str) -> str: ...

    @abstractmethod
    def get_list_of_primary_key_columns_for_table(self, database: str, schema: str, table: str) -> list: ...

    @abstractmethod
    def get_records_from_table(self, database: str, schema: str, table: str, where_clause: str) -> dict: ...

    @abstractmethod
    def update_rows(
            self, database: str, schema: str, table: str, set_clause: str, where_clause: str
    ) -> None: ...

    @abstractmethod
    def update_date_column_with_random_variance(
            self,
            database: str,
            schema: str,
            table: str,
            column: str,
            where_clause: str,
            range_min: int,
            range_max: int
    ) -> None: ...

    @abstractmethod
    def truncate_table(self, database: str, schema: str, table: str) -> None: ...

    @abstractmethod
    def delete_rows(self, database: str, schema: str, table: str, where_clause: str) -> None: ...

    @abstractmethod
    def execute_command(self, command: str) -> None: ...

    @abstractmethod
    def disable_all_triggers_for_database(self, database: str) -> None: ...

    @abstractmethod
    def disable_all_triggers_for_table(self, database: str, schema: str, table: str) -> None: ...

    @abstractmethod
    def disable_single_trigger_for_table(self, database: str, schema: str, table: str, trigger: str) -> None: ...

    @abstractmethod
    def enable_all_triggers_for_database(self, database: str) -> None: ...

    @abstractmethod
    def enable_all_triggers_for_table(self, database: str, schema: str, table: str) -> None: ...

    @abstractmethod
    def enable_single_trigger_for_table(self, database: str, schema: str, table: str, trigger: str) -> None: ...

    @abstractmethod
    def disable_all_check_constraints_for_database(self, database: str) -> None: ...

    @abstractmethod
    def disable_all_check_constraints_for_table(self, database: str, schema: str, table: str) -> None: ...

    @abstractmethod
    def disable_single_check_constraint_for_table(
            self, database: str, schema: str, table: str, check_constraint: str) -> None: ...

    @abstractmethod
    def enable_all_check_constraints_for_database(self, database: str) -> None: ...

    @abstractmethod
    def enable_all_check_constraints_for_table(self, database: str, schema: str, table: str) -> None: ...

    @abstractmethod
    def enable_single_check_constraint_for_table(
            self,
            database: str,
            schema: str,
            table: str,
            check_constraint: str
    ) -> None: ...

    @abstractmethod
    def disable_all_foreign_keys_for_database(self, database: str) -> None: ...

    @abstractmethod
    def disable_all_foreign_keys_for_table(self, database: str, schema: str, table: str) -> None: ...

    @abstractmethod
    def disable_single_foreign_key_for_table(self, database: str, schema: str, table: str, foreign_key) -> None: ...

    @abstractmethod
    def enable_all_foreign_keys_for_database(self, database: str) -> None: ...

    @abstractmethod
    def enable_all_foreign_keys_for_table(self, database: str, schema: str, table: str) -> None: ...

    @abstractmethod
    def enable_single_foreign_key_for_table(self, database: str, schema: str, table: str, foreign_key) -> None: ...
