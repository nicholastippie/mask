from mask.configuration.constants import Constants
from mask.utilities.file import generate_dict_from_json
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

    def _get_records_and_primary_key(self, where_clause: str) -> tuple[dict, list]:
        if where_clause is None or where_clause == "":
            where_clause = Constants.DEFAULT_WHERE_CLAUSE

        records: dict = self.database_gateway.get_records_from_table(
            database=self.database,
            schema=self.schema,
            table=self.table,
            where_clause=where_clause
        )

        primary_key: list[str] = self.database_gateway.get_primary_key_for_table(
            database=self.database,
            schema=self.schema,
            table=self.table
        )

        return records, primary_key

    def _update_record(self, record: dict, primary_key: list, **kwargs) -> None:
        if "mapping" in kwargs and "replacement_values" in kwargs:
            set_clause, set_clause_values = self.database_gateway.generate_set_clause_from_mapping(
                mapping=kwargs["mapping"],
                replacement_values=kwargs["replacement_values"]
            )
        elif "column" in kwargs and "replacement_value" in kwargs:
            set_clause, set_clause_values = self.database_gateway.generate_set_clause_for_column(
                column=kwargs["column"],
                replacement_value=kwargs["replacement_value"]
            )
        else:
            raise ValueError(f"Did not get expected parameter set to update record")

        where_clause, where_clause_values = self.database_gateway.generate_where_clause_from_record(
            record=record,
            primary_key=primary_key
        )

        values: tuple[any] = set_clause_values + where_clause_values

        self.database_gateway.update_rows(
            database=self.database,
            schema=self.schema,
            table=self.table,
            set_clause=set_clause,
            where_clause=where_clause,
            values=values
        )


@dataclass
class DynamicValueSubstitutionRule(DataRule):
    """ Replaces values in columns with data from a data set """

    data_mapping: dict = None
    where_clause: str = Constants.DEFAULT_WHERE_CLAUSE
    dataset_path: str = ""

    def validate_instructions(self) -> None:
        super().validate_instructions()
        if self.data_mapping is None or self.data_mapping == {}:
            raise ValueError(f"'data_mapping' property not set for {self}")
        if not exists(self.dataset_path):
            raise FileNotFoundError(f"Could not find data set at '{self.dataset_path}' for {self}")

    def execute(self) -> None:
        dataset: dict = generate_dict_from_json(self.dataset_path)

        # The system is designed to handle only one set of database-to-dataset
        # mappings; however, it is possible to put in multiple in the JSON array.
        # But this does not make sense for how this rule is implemented. So, let's
        # grab only the first and use that. The documentation explains that only
        # one mapping set should be provided.
        mapping: dict = self.data_mapping[0]

        records, primary_key = super()._get_records_and_primary_key(where_clause=self.where_clause)

        print(f"Records={len(records)} - {self}")

        count = 0
        for record in records:
            replacement_values: dict = choice(dataset)

            super()._update_record(
                record=record,
                primary_key=primary_key,
                mapping=mapping,
                replacement_values=replacement_values
            )

            count = count + 1
            if count % 1000 == 0:
                print(f"Count={count} @ {datetime.now()} - {self}")


@dataclass
class StaticValueSubstitutionRule(DataRule):
    """ Replaces the values in a column with a static value """

    column: str = ""
    static_value: any = ""
    where_clause: str = ""

    def validate_instructions(self) -> None:
        super().validate_instructions()
        if self.column == "":
            raise ValueError(f"'column' property not set for {self}")

    def execute(self) -> None:
        replacement_value = self.static_value if self.static_value != "NULL" else None

        set_clause, set_clause_values = self.database_gateway.generate_set_clause_for_column(
            column=self.column,
            replacement_value=replacement_value
        )

        # Let's assume that the where clause passed in from the instruction set
        # is appropriately sanitized (e.g. no single-quote issues). Only the
        # set clause needs to be sanitized.
        self.database_gateway.update_rows(
            database=self.database,
            schema=self.schema,
            table=self.table,
            set_clause=set_clause,
            where_clause=self.where_clause,
            values=set_clause_values
        )


@dataclass
class FakeSsnSubstitutionRule(DataRule):
    """ Replaces an existing Social Security Number with a fake one """

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

        records, primary_key = super()._get_records_and_primary_key(where_clause=select_where_clause)

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

            super()._update_record(
                record=record,
                primary_key=primary_key,
                column=self.column,
                replacement_value=invalid_ssn
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

        records, primary_key = super()._get_records_and_primary_key(where_clause=select_where_clause)

        count = 0
        for record in records:
            if record[self.column] is None:
                continue

            random_number: int = randint(1, self.range) if self.range > 0 else randint(self.range, -1)
            replacement_date: datetime = record[self.column] + timedelta(days=random_number)
            replacement_date = replacement_date.replace(microsecond=0)

            super()._update_record(
                record=record,
                primary_key=primary_key,
                column=self.column,
                replacement_value=replacement_date
            )

            count = count + 1
            if count % 1000 == 0:
                print(f"Count={count} @ {datetime.now()} - {self}")


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
