from mask.rules.rule import Rule
from mask.database_access.database_gateway import DatabaseGateway
from dataclasses import dataclass


@dataclass
class DisableTriggerRule(Rule):
    """ Disables one or more table triggers in a database """

    database: str = ""
    schema: str = ""
    table: str = ""
    trigger: str = ""
    database_gateway: DatabaseGateway = None

    def __str__(self) -> str:
        return f"{__class__.__name__} with database='{self.database}', " \
               f"schema='{self.schema}', table='{self.table}', trigger='{self.trigger}'"

    def validate_instructions(self) -> None:
        if self.database == "*":
            raise ValueError(f"Wildcard character is not allowed for 'database' property "
                             f"for {self}")
        if self.table == "*":
            raise ValueError(f"Wildcard character is not allowed for 'table' property "
                             f"for {self}")

    def execute(self) -> None:
        """
        This rule is the reverse of EnableTriggerRule.

        The instruction set has four properties for this rule: database,
        schema, table, and trigger. A wildcard character (*) can be used
        to tell the masking engine to disable all triggers for the database
        or for a specific table. This character does not handle partial namesS
        for any properties (e.g., "*_audit" for all audit tables). Think
        "select * from", not "where first_name like '%ohn'".

        A wildcard character for "database" is illegal, since this rule will
        not disable all triggers across all database, but only within one
        database at a time. The "database" value tells the masking engine
        which database context to use.

        A wildcard character for "schema" means disable all triggers within
        all schema (and tables) in the database: That is, disable all triggers
        in the database. The "table" and "trigger" properties are ignored if
        this wildcard is used.

        A wildcard character for "table" is illegal, since this situation is
        not supported: The masking engine will not disable all triggers for
        all tables within a schema. This could be a future feature.

        A wildcard character for "trigger" will disable all triggers for
        the provided table, assuming schema and table are valid database
        objects.

        Note that this rule disables triggers associated with tables and not
        database-level or higher-level triggers. Use DisableDatabaseTriggerRule
        and DisableServerTriggerRule, respectively.

        :return: None
        """
        if self.schema == "*":
            self.database_gateway.disable_all_triggers_for_database(self.database)
            return

        if self.trigger == "*":
            self.database_gateway.disable_all_triggers_for_table(
                database=self.database,
                schema=self.schema,
                table=self.table
            )
            return

        self.database_gateway.disable_single_trigger_for_table(
            database=self.database,
            schema=self.schema,
            table=self.table,
            trigger=self.trigger
        )


@dataclass
class EnableTriggerRule(Rule):
    """ Enables one or more table triggers in a database """

    database: str = ""
    schema: str = ""
    table: str = ""
    trigger: str = ""
    database_gateway: DatabaseGateway = None

    def __str__(self) -> str:
        return f"{__class__.__name__} with database='{self.database}', " \
               f"schema='{self.schema}', table='{self.table}', trigger='{self.trigger}'"

    def validate_instructions(self) -> None:
        if self.database == "*":
            raise ValueError(f"Wildcard character is not allowed for 'database' property "
                             f"for {self}")
        if self.table == "*":
            raise ValueError(f"Wildcard character is not allowed for 'table' property "
                             f"for {self}")

    def execute(self) -> None:
        """
        This rule is the reverse of DisableTriggerRule.

        The instruction set has four properties for this rule: database,
        schema, table, and trigger. A wildcard character (*) can be used
        to tell the masking engine to enable all triggers for the database
        or for a specific table. This character does not handle partial names
        for any properties (e.g., "*_audit" for all audit tables). Think
        "select * from", not "where first_name like '%ohn'".

        A wildcard character for "database" is illegal, since this rule will
        not enable all triggers across all database, but only within one
        database at a time. The "database" value tells the masking engine
        which database context to use.

        A wildcard character for "schema" means enable all triggers within
        all schema (and tables) in the database: That is, enable all triggers
        in the database. The "table" and "trigger" properties are ignored if
        this wildcard is used.

        A wildcard character for "table" is illegal, since this situation is
        not supported: The masking engine will not enable all triggers for
        all tables within a schema. This could be a future feature.

        A wildcard character for "trigger" will enable all triggers for
        the provided table, assuming schema and table are valid database
        objects.

        Note that this rule enables triggers associated with tables and not
        database-level or higher-level triggers. Use EnableDatabaseTriggerRule
        and EnableServerTriggerRule, respectively.

        :return: None
        """
        if self.schema == "*":
            self.database_gateway.enable_all_triggers_for_database(self.database)
            return

        if self.trigger == "*":
            self.database_gateway.enable_all_triggers_for_table(
                database=self.database,
                schema=self.schema,
                table=self.table
            )
            return

        self.database_gateway.enable_single_trigger_for_table(
            database=self.database,
            schema=self.schema,
            table=self.table,
            trigger=self.trigger
        )


@dataclass
class DisableCheckConstraintRule(Rule):
    """ Disable one or more table check constraints in a database """

    database: str = ""
    schema: str = ""
    table: str = ""
    check_constraint: str = ""
    database_gateway: DatabaseGateway = None

    def __str__(self) -> str:
        return f"{__class__.__name__} with database='{self.database}', " \
               f"schema='{self.schema}', table='{self.table}', check_constraint='{self.check_constraint}'"

    def validate_instructions(self) -> None:
        if self.database == "*":
            raise ValueError(f"Wildcard character is not allowed for 'database' property "
                             f"for {self}")
        if self.table == "*":
            raise ValueError(f"Wildcard character is not allowed for 'table' property "
                             f"for {self}")

    def execute(self) -> None:
        if self.schema == "*":
            self.database_gateway.disable_all_check_constraints_for_database(self.database)
            return

        if self.check_constraint == "*":
            self.database_gateway.disable_all_check_constraints_for_table(
                database=self.database,
                schema=self.schema,
                table=self.table
            )
            return

        self.database_gateway.disable_single_check_constraint_for_table(
            database=self.database,
            schema=self.schema,
            table=self.table,
            check_constraint=self.check_constraint
        )


@dataclass
class EnableCheckConstraintRule(Rule):
    """ Enable one or more table check constraints in a database """

    database: str = ""
    schema: str = ""
    table: str = ""
    check_constraint: str = ""
    database_gateway: DatabaseGateway = None

    def __str__(self) -> str:
        return f"{__class__.__name__} with database='{self.database}', " \
               f"schema='{self.schema}', table='{self.table}', check_constraint='{self.check_constraint}'"

    def validate_instructions(self) -> None:
        if self.database == "*":
            raise ValueError(f"Wildcard character is not allowed for 'database' property "
                             f"for {self}")
        if self.table == "*":
            raise ValueError(f"Wildcard character is not allowed for 'table' property "
                             f"for {self}")

    def execute(self) -> None:
        if self.schema == "*":
            self.database_gateway.enable_all_check_constraints_for_database(self.database)
            return

        if self.check_constraint == "*":
            self.database_gateway.enable_all_check_constraints_for_table(
                database=self.database,
                schema=self.schema,
                table=self.table
            )
            return

        self.database_gateway.enable_single_check_constraint_for_table(
            database=self.database,
            schema=self.schema,
            table=self.table,
            check_constraint=self.check_constraint
        )


@dataclass
class DisableForeignKeyRule(Rule):
    """ Disable one or more foreign keys in a database """

    database: str = ""
    schema: str = ""
    table: str = ""
    foreign_key: str = ""
    database_gateway: DatabaseGateway = None

    def __str__(self) -> str:
        return f"{__class__.__name__} with database='{self.database}', " \
               f"schema='{self.schema}', table='{self.table}', foreign_key='{self.foreign_key}'"

    def validate_instructions(self) -> None:
        if self.database == "*":
            raise ValueError(f"Wildcard character is not allowed for 'database' property "
                             f"for {self}")
        if self.table == "*":
            raise ValueError(f"Wildcard character is not allowed for 'table' property "
                             f"for {self}")

    def execute(self) -> None:
        if self.schema == "*":
            self.database_gateway.disable_all_foreign_keys_for_database(self.database)
            return

        if self.foreign_key == "*":
            self.database_gateway.disable_all_foreign_keys_for_table(
                database=self.database,
                schema=self.schema,
                table=self.table
            )
            return

        self.database_gateway.disable_single_foreign_key_for_table(
            database=self.database,
            schema=self.schema,
            table=self.table,
            foreign_key=self.foreign_key
        )


@dataclass
class EnableForeignKeyRule(Rule):
    """ Enable one or more foreign keys in a database """

    database: str = ""
    schema: str = ""
    table: str = ""
    foreign_key: str = ""
    database_gateway: DatabaseGateway = None

    def __str__(self) -> str:
        return f"{__class__.__name__} with database='{self.database}', " \
               f"schema='{self.schema}', table='{self.table}', foreign_key='{self.foreign_key}'"

    def validate_instructions(self) -> None:
        if self.database == "*":
            raise ValueError(f"Wildcard character is not allowed for 'database' property "
                             f"for {self}")
        if self.table == "*":
            raise ValueError(f"Wildcard character is not allowed for 'table' property "
                             f"for {self}")

    def execute(self) -> None:
        if self.schema == "*":
            self.database_gateway.enable_all_foreign_keys_for_database(self.database)
            return

        if self.foreign_key == "*":
            self.database_gateway.enable_all_foreign_keys_for_table(
                database=self.database,
                schema=self.schema,
                table=self.table
            )
            return

        self.database_gateway.enable_single_foreign_key_for_table(
            database=self.database,
            schema=self.schema,
            table=self.table,
            foreign_key=self.foreign_key
        )