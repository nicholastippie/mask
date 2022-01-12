from mask.config.constants import Constants
from mask.file import generate_dict_from_json
from mask.rules.rule import Rule

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from os.path import exists
from random import randint, choice


@dataclass
class DataRule(Rule):
    database: str = ""
    schema: str = ""
    table: str = ""

    def validate_instructions(self) -> None:
        super().validate_instructions()
        if self.database == "":
            raise ValueError(f"'database' property not set for {self}")
        if self.schema == "":
            raise ValueError(f"'schema' property not set for {self}")
        if self.table == "":
            raise ValueError(f"'table' property not set for {self}")

    def execute(self) -> None:
        pass


@dataclass
class FakeStringSubstitutionRule(DataRule):
    """ Replaces values in a column with data from a data set """

    column: str = ""
    where_clause: str = ""
    data_set_path: str = ""
    data_set_key: str = ""

    def validate_instructions(self) -> None:
        super().validate_instructions()
        if self.column == "":
            raise ValueError(f"'column' property not set for {self}")
        if not exists(self.data_set_path):
            raise FileNotFoundError(f"Could not find data set at '{self.data_set_path}' for {self}")
        if self.data_set_key == "":
            raise ValueError(f"'data_set_key' property not set for {self}")

    def execute(self):
        data_set_complete: dict = generate_dict_from_json(self.data_set_path)
        data_set_values_only: list[str] = list()
        for item in data_set_complete:
            data_set_values_only.append(item[self.data_set_key])

        records: dict = self.database_gateway.get_records_from_table(
            database=self.database,
            schema=self.schema,
            table=self.table,
            where_clause=self.where_clause
        )

        primary_key: list[str] = self.database_gateway.get_list_of_primary_key_columns_for_table(
            database=self.database,
            schema=self.schema,
            table=self.table
        )

        count = 0
        for record in records:
            if record[self.column] is None:
                continue

            replacement_value: str = choice(data_set_values_only)

            self.database_gateway.update_rows(
                database=self.database,
                schema=self.schema,
                table=self.table,
                set_clause=self.database_gateway.generate_update_set_clause_for_column(
                    column=self.column,
                    replacement_value=replacement_value
                ),
                where_clause=self.database_gateway.generate_where_clause_from_record(
                    record=record,
                    primary_key=primary_key
                ),
            )

            count = count + 1
            if count % 1000 == 0:
                print(f"Count={count} @ {datetime.now()}")


@dataclass
class StaticStringSubstitutionRule(DataRule):
    """ Replaces the values in a column with a static string value """

    column: str = ""
    static_value: str = ""
    where_clause: str = ""

    def validate_instructions(self) -> None:
        super().validate_instructions()
        if self.column == "":
            raise ValueError(f"'column' property not set for {self}")

    def execute(self) -> None:
        replacement_value = self.static_value if self.static_value != "NULL" else None

        self.database_gateway.update_rows(
            database=self.database,
            schema=self.schema,
            table=self.table,
            set_clause=self.database_gateway.generate_update_set_clause_for_column(
                column=self.column,
                replacement_value=replacement_value
            ),
            where_clause=self.where_clause
        )


@dataclass
class FakeSsnSubstitutionRule(DataRule):
    """ Generates a random invalid Social Security Number """

    class IgnoreNullOptions(Enum):
        AFFIRMATIVE = "yes"
        NEGATIVE = "no"

    column: str = ""
    seperator: str = ""
    ignore_null: str = ""

    def validate_instructions(self) -> None:
        super().validate_instructions()
        if self.column == "":
            raise ValueError(f"'column' property not set for {self}")
        if self.ignore_null != self.IgnoreNullOptions.AFFIRMATIVE.value \
                and self.ignore_null != self.IgnoreNullOptions.NEGATIVE.value:
            raise ValueError(f"'ignore_null' property must be either "
                             f"'{self.IgnoreNullOptions.AFFIRMATIVE.value}' "
                             f"or '{self.IgnoreNullOptions.NEGATIVE.value}' for {self}")

    def execute(self) -> None:
        list_of_used_invalid_ssns: list[str] = list()
        select_where_clause: str = Constants.DEFAULT_WHERE_CLAUSE

        if self.ignore_null == self.IgnoreNullOptions.AFFIRMATIVE.value:
            select_where_clause = self.database_gateway.append_where_column_is_not_null(
                column=self.column,
                where_clause=select_where_clause
            )

        records: dict = self.database_gateway.get_records_from_table(
            database=self.database,
            schema=self.schema,
            table=self.table,
            where_clause=select_where_clause
        )

        primary_key: list[str] = self.database_gateway.get_list_of_primary_key_columns_for_table(
            database=self.database,
            schema=self.schema,
            table=self.table
        )

        count: int = 0
        for record in records:
            invalid_ssn: str = ""
            is_unique: bool = False
            retry_attempts: int = 0
            while not is_unique:
                if retry_attempts >= Constants.MAX_RETRY_ATTEMPTS:
                    raise ValueError(f"Could not find a unique invalid SSN within the allowed retry attempts")

                invalid_ssn = self._generate_invalid_ssn()

                if invalid_ssn in list_of_used_invalid_ssns:
                    retry_attempts = retry_attempts + 1
                    is_unique = False
                else:
                    list_of_used_invalid_ssns.append(invalid_ssn)
                    is_unique = True

            if invalid_ssn == "" or invalid_ssn is None:
                raise ValueError(f"Invalid SSN cannot be empty")

            self.database_gateway.update_rows(
                database=self.database,
                schema=self.schema,
                table=self.table,
                set_clause=self.database_gateway.generate_update_set_clause_for_column(
                    column=self.column,
                    replacement_value=invalid_ssn
                ),
                where_clause=self.database_gateway.generate_where_clause_from_record(
                    record=record,
                    primary_key=primary_key
                )
            )

            count = count + 1
            if count % 1000 == 0:
                print(f"Count={count} @ {datetime.now()} for {self}")

    def _generate_invalid_ssn(self) -> str:
        """
        Returns an invalid Social Security Number (SSN).

        An invalid SSN has at least one of the following characteristics:
            - Begins with the number 9
            - Begins with 666 in positions 1-3
            - Begins with 000 in positions 1-3
            - Contains 00 in positions 4-5
            - Contains 0000 in positions 6-9

        The following algorithm ensures that one of those characteristics is met.

        Additionally, return an invalid SSN that is also not a valid U.S. Individual
        Taxpayer Identification Number (ITIN), which have some groups numbers between
        70 and 99.

        Reference: https://www.ssa.gov/kc/SSAFactSheet--IssuingSSNs.pdf

        :return: An invalid SSN matching the criteria above
        """
        itin_group_numbers: list[int] = [
            70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
            80, 81, 82, 83, 84, 85, 86, 87, 88,
            90, 91, 92, 94, 95, 96, 97, 98, 99,
        ]

        area: int = randint(0, 999)
        group: int = randint(0, 99)
        serial: int = randint(0, 9999)

        if area >= 900:
            group = choice([x for x in range(0, 100) if x not in itin_group_numbers])
        elif area < 900 and area not in {666, 0}:
            zero_group_or_serial: str = choice(["group", "serial"])
            group = 0 if zero_group_or_serial == "group" else group
            serial = 0 if zero_group_or_serial == "serial" else serial

        return f"{area:03d}{self.seperator}{group:02d}{self.seperator}{serial:04d}"


@dataclass
class DateVarianceRule(DataRule):
    """ Move a date by a random amount within the specified bounds """

    class Method(Enum):
        SIMPLE = "simple"
        COMPLETE = "complete"

    column: str = ""
    range: int = 0
    where_clause: str = ""
    method: str = ""

    def validate_instructions(self) -> None:
        super().validate_instructions()
        if self.column == "":
            raise ValueError(f"'column' property not set for {self}")
        if self.range == 0:
            raise ValueError(f"'range' property must be either greater than or less than 0 "
                             f"for {self}")
        if self.method != self.Method.SIMPLE.value and self.method != self.Method.COMPLETE.value:
            raise ValueError(f"'method' must be either set to '{self.Method.SIMPLE.value}' "
                             f"or '{self.Method.COMPLETE.value}' for {self}")

    def execute(self) -> None:
        if self.method == self.Method.SIMPLE.value:
            self._execute_simple_method()
        elif self.method == self.Method.COMPLETE.value:
            self._execute_complete_method()
        else:
            raise NotImplementedError(f"'{self.method}' is not recognized as a valid operation for {self}")

    def _execute_simple_method(self) -> None:
        """
        Executes date variance using a set-based approach, which means that
        all dates within the column will be changed by the same random number
        of days, potentially reducing the effectiveness of the masking but
        greatly speeding up the operation over the "complete" method.

        :return: None
        """
        range_min = 1 if self.range > 0 else -1

        where_clause: str = self.where_clause if self.where_clause != "" else Constants.DEFAULT_WHERE_CLAUSE

        self.database_gateway.update_date_column_with_random_variance(
            database=self.database,
            schema=self.schema,
            table=self.table,
            column=self.column,
            where_clause=self.database_gateway.append_where_column_is_not_null(
                column=self.column,
                where_clause=where_clause
            ),
            range_min=range_min,
            range_max=self.range
        )

    def _execute_complete_method(self) -> None:
        """
        Executes date variance using a row-by-row approach, which means that
        each date value will be random within the given range. This method
        is *much* slower than the "simple" method by orders of magnitude,
        but it more thoroughly masks the data with randomness.

        :return: None
        """
        select_where_clause: str = self.where_clause if self.where_clause != "" else Constants.DEFAULT_WHERE_CLAUSE
        select_where_clause = self.database_gateway.append_where_column_is_not_null(
            column=self.column,
            where_clause=select_where_clause
        )

        records: dict = self.database_gateway.get_records_from_table(
            database=self.database,
            schema=self.schema,
            table=self.table,
            where_clause=select_where_clause
        )

        primary_key: list[str] = self.database_gateway.get_list_of_primary_key_columns_for_table(
            database=self.database,
            schema=self.schema,
            table=self.table
        )

        count = 0
        for record in records:
            if record[self.column] is None:
                continue

            random_number: int = randint(1, self.range) if self.range > 0 else randint(self.range, -1)
            replacement_date: datetime = record[self.column] + timedelta(days=random_number)
            replacement_date = replacement_date.replace(microsecond=0)

            self.database_gateway.update_rows(
                database=self.database,
                schema=self.schema,
                table=self.table,
                set_clause=self.database_gateway.generate_update_set_clause_for_column(
                    column=self.column,
                    replacement_value=replacement_date
                ),
                where_clause=self.database_gateway.generate_where_clause_from_record(
                    record=record,
                    primary_key=primary_key
                ),
            )

            count = count + 1
            if count % 1000 == 0:
                print(f"Count={count} @ {datetime.now()} for {self}")


@dataclass
class TruncateTableRule(DataRule):
    """ Truncates the specified table in a database """

    def execute(self):
        self.database_gateway.truncate_table(self.database, self.schema, self.table)


@dataclass
class DeleteRowsRule(DataRule):
    """ Deletes all rows in a table or only those specified in a where clause """

    where_clause: str = ""

    def execute(self):
        self.database_gateway.delete_rows(
            self.database, self.schema, self.table, self.where_clause
        )
