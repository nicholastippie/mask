from mask.database.database_context import DatabaseContext
from mask.database.database_gateway import DatabaseGateway


class PostgresDatabaseGateway(DatabaseGateway):
    def __init__(self, database_context: DatabaseContext):
        self._database_context: DatabaseContext = database_context

    def generate_where_clause_from_record(self, record: dict, primary_key: list) -> str:
        pass

    def generate_update_set_clause_for_column(self, column: str, replacement_value: vars) -> str:
        pass

    def generate_update_set_clause_for_columns_from_mapping(
            self,
            mapping: dict,
            replacement_values: dict) -> str:
        pass

    def append_where_column_is_not_null(self, column: str, where_clause: str) -> str:
        pass

    def get_list_of_primary_key_columns_for_table(self, database: str, schema: str, table: str) -> list:
        pass

    def get_records_from_table(self, database: str, schema: str, table: str, where_clause: str) -> dict:
        pass

    def update_rows(
            self,
            database: str,
            schema: str,
            table: str,
            set_clause: str,
            where_clause: str
    ) -> None:
        pass

    def update_date_column_with_random_variance(
            self, database: str, schema: str, table: str, column: str, where_clause: str,
            range_min: int, range_max: int
    ) -> None:
        pass

    def truncate_table(self, database: str, schema: str, table: str) -> None:
        self._database_context.execute(
            query=f'truncate table "{database}"."{schema}"."{table}";',
            values=None
        )

    def delete_rows(self, database: str, schema: str, table: str, where_clause: str) -> None:
        self._database_context.execute(
            query=f'delete from "{database}"."{schema}"."{table}" {where_clause};'
        )

    def execute_command(self, command: str) -> None:
        pass

    def disable_all_triggers_for_database(self, database: str) -> None:
        pass

    def disable_all_triggers_for_table(self, database: str, schema: str, table: str) -> None:
        pass

    def disable_single_trigger_for_table(self, database: str, schema: str, table: str, trigger: str) -> None:
        pass

    def enable_all_triggers_for_database(self, database: str) -> None:
        pass

    def enable_all_triggers_for_table(self, database: str, schema: str, table: str) -> None:
        pass

    def enable_single_trigger_for_table(self, database: str, schema: str, table: str, trigger: str) -> None:
        pass

    def disable_all_check_constraints_for_database(self, database: str) -> None:
        pass

    def disable_all_check_constraints_for_table(self, database: str, schema: str, table: str) -> None:
        pass

    def disable_single_check_constraint_for_table(
            self, database: str, schema: str, table: str, check_constraint: str) -> None:
        pass

    def enable_all_check_constraints_for_database(self, database: str) -> None:
        pass

    def enable_all_check_constraints_for_table(self, database: str, schema: str, table: str) -> None:
        pass

    def enable_single_check_constraint_for_table(
            self, database: str, schema: str, table: str, check_constraint: str) -> None:
        pass

    def disable_all_foreign_keys_for_database(self, database: str) -> None:
        pass

    def disable_all_foreign_keys_for_table(self, database: str, schema: str, table: str) -> None:
        pass

    def disable_single_foreign_key_for_table(self, database: str, schema: str, table: str, foreign_key) -> None:
        pass

    def enable_all_foreign_keys_for_database(self, database: str) -> None:
        pass

    def enable_all_foreign_keys_for_table(self, database: str, schema: str, table: str) -> None:
        pass

    def enable_single_foreign_key_for_table(self, database: str, schema: str, table: str, foreign_key) -> None:
        pass


